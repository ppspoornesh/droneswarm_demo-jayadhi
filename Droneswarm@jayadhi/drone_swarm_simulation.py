import paho.mqtt.client as mqtt
import json
import time
import random
from threading import Thread, Lock
from math import radians, sin, cos, sqrt, atan2


class Drone:
    def __init__(self, drone_id, position, battery=100):
        self.drone_id = drone_id
        self.position = position  # [lat, lon, alt]
        self.velocity = [0.0, 0.0, 0.0]
        self.battery = battery
        self.status = "MOVING"
        self.task = f"Waypoint_{drone_id}"
        self.waypoint = self._generate_waypoint()

        self.last_heard = {}
        self.task_registry = {self.drone_id: self.task}
        self.lock = Lock()

        # Setup MQTT
        self.client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2, client_id=drone_id)
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message
        self._connect_with_retry()

    def _connect_with_retry(self):
        while True:
            try:
                self.client.connect("broker.hivemq.com", 1883, 60)
                self.client.subscribe("swarm/#", qos=1)
                print(f"[{self.drone_id}] Connected to MQTT broker.")
                break
            except Exception as e:
                print(f"[{self.drone_id}] MQTT connection failed: {e}. Retrying...")
                time.sleep(5)

    def _on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            print(f"[{self.drone_id}] Reconnected successfully.")
            self.client.subscribe("swarm/#", qos=1)
        else:
            print(f"[{self.drone_id}] Failed to connect. Code: {rc}")

    def _on_message(self, client, userdata, msg):
        try:
            data = json.loads(msg.payload.decode())
            if not isinstance(data, dict) or data.get("drone_id") == self.drone_id:
                return

            with self.lock:
                peer_id = data["drone_id"]
                self.last_heard[peer_id] = time.time()
                self.task_registry[peer_id] = data.get("task", "")

                # Collision detection
                distance = self._haversine(self.position, data["position"])
                if distance < 2.0:
                    print(f"[{self.drone_id}] Collision alert with {peer_id} ({distance:.2f}m).")
                    if self.drone_id < peer_id:
                        print(f"[{self.drone_id}] Takes priority.")
                    else:
                        self.position[2] += 5.0
                        self.battery -= 0.5
                        print(f"[{self.drone_id}] Gained altitude to {self.position[2]}m.")

                # Task conflict resolution
                if data["task"] == self.task:
                    if self.battery > data["battery"]:
                        print(f"[{self.drone_id}] Retaining task: {self.task}")
                    else:
                        self.task = f"Waypoint_{self.drone_id}_{random.randint(1000, 9999)}"
                        self.task_registry[self.drone_id] = self.task
                        self.waypoint = self._generate_waypoint()
                        print(f"[{self.drone_id}] Task reassigned to: {self.task}")

                # Check for lost drones
                for peer_id, last_seen in list(self.last_heard.items()):
                    if time.time() - last_seen > 6.0:
                        print(f"[{self.drone_id}] Lost contact with {peer_id}.")
                        del self.last_heard[peer_id]
                        del self.task_registry[peer_id]
                        self._elect_leader()

        except (json.JSONDecodeError, ValueError) as e:
            print(f"[{self.drone_id}] Invalid message received: {e}")

    def _elect_leader(self):
        print(f"[{self.drone_id}] Starting leader election.")
        with self.lock:
            all_ids = list(self.last_heard.keys()) + [self.drone_id]
            self.status = "LEADER" if self.drone_id == min(all_ids) else "FOLLOWER"
            print(f"[{self.drone_id}] New role: {self.status}")

    def _haversine(self, pos1, pos2):
        R = 6371000  # meters
        lat1, lon1, alt1 = pos1
        lat2, lon2, alt2 = pos2

        dlat = radians(lat2 - lat1)
        dlon = radians(lon2 - lon1)

        a = sin(dlat/2)**2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon/2)**2
        c = 2 * atan2(sqrt(a), sqrt(1 - a))

        horizontal = R * c
        vertical = alt2 - alt1
        return sqrt(horizontal**2 + vertical**2)

    def _generate_waypoint(self):
        lat_offset = random.uniform(-0.01, 0.01)
        lon_offset = random.uniform(-0.01, 0.01)
        alt = random.uniform(10, 50)
        return [self.position[0] + lat_offset, self.position[1] + lon_offset, alt]

    def _move_towards_waypoint(self):
        for i in range(2):
            if abs(self.position[i] - self.waypoint[i]) > 0.00001:
                step = 0.0001 if self.position[i] < self.waypoint[i] else -0.0001
                self.position[i] += step
                self.velocity[i] = step
            else:
                self.velocity[i] = 0.0

        if abs(self.position[2] - self.waypoint[2]) > 0.1:
            step = 0.1 if self.position[2] < self.waypoint[2] else -0.1
            self.position[2] += step
            self.velocity[2] = step
        else:
            self.velocity[2] = 0.0

        if self._haversine(self.position, self.waypoint) < 1.0:
            self.waypoint = self._generate_waypoint()
            print(f"[{self.drone_id}] Waypoint reached. New target: {self.waypoint}")

    def broadcast(self):
        while self.battery > 10:
            with self.lock:
                self._move_towards_waypoint()

                message = {
                    "drone_id": self.drone_id,
                    "position": [round(p, 6) for p in self.position],
                    "velocity": [round(v, 6) for v in self.velocity],
                    "battery": round(self.battery, 1),
                    "status": self.status,
                    "task": self.task,
                    "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                    "checksum": "0x4A3B"
                }

                try:
                    self.client.publish("swarm", json.dumps(message), qos=1)
                    print(f"[{self.drone_id}] Broadcasted: {message}")
                except Exception as e:
                    print(f"[{self.drone_id}] Publish error: {e}")

                self.battery -= 0.2
                if any(v != 0 for v in self.velocity):
                    self.battery -= 0.3

            time.sleep(1)

        with self.lock:
            self.status = "LOW_BATTERY"
            print(f"[{self.drone_id}] Battery low. Status set to LOW_BATTERY.")


def simulate_drone(drone_instance):
    drone_instance.client.loop_start()
    drone_instance.broadcast()


if __name__ == "__main__":
    drone_fleet = [
        Drone("D1", [23.0701, 72.5597, 10.0], 90),
        Drone("D2", [23.0702, 72.5598, 10.0], 85),
        Drone("D3", [23.0701, 72.5597, 10.0], 80)
    ]

    threads = [Thread(target=simulate_drone, args=(drone,)) for drone in drone_fleet]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
