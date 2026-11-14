import csv
import math
import os
import random

# Ensure datasets directory exists
os.makedirs("datasets", exist_ok=True)
random.seed(42)
n_samples = 3000

durations = [15, 20, 30, 45, 60, 90, 120, 180]
volumes = [0.5, 0.8, 1.0, 1.5, 2.0, 3.0, 5.0, 10.0]

output_file = os.path.join("datasets", "sanmist_telemetry_synthetic.csv")

class_counts = {0: 0, 1: 0, 2: 0}

with open(output_file, mode="w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow([
        "ambient_temp_c",
        "relative_humidity_pct",
        "flow_rate_ml_min",
        "cycle_duration_sec",
        "room_volume_m3",
        "sei_score",
        "efficacy_class"
    ])

    for _ in range(n_samples):
        temp = round(random.uniform(18.0, 38.0), 2)
        hum = round(random.uniform(40.0, 90.0), 2)
        flow = round(random.uniform(12.0, 48.0), 2)
        duration = random.choice(durations)
        volume = random.choice(volumes)

        # Evaporation loss adjustment (temperature and humidity influence)
        evap_loss = (temp - 25.0) * 0.003 - (hum - 65.0) * 0.001
        loss_factor = max(0.85, min(1.15, 1.0 - evap_loss))
        effective_flow = flow * loss_factor

        # Standard SEI calculation matching Backend & Firmware
        duration_min = duration / 60.0
        sei = round((effective_flow * duration_min) / volume, 3)

        # PD 856 & RA 10611 Threshold Standards:
        # Class 2: Pass (SEI >= 2.800)
        # Class 1: Marginal (1.800 <= SEI < 2.800)
        # Class 0: Fail (SEI < 1.800)
        if sei >= 2.800:
            label = 2
        elif sei >= 1.800:
            label = 1
        else:
            label = 0

        class_counts[label] += 1
        writer.writerow([temp, hum, flow, duration, volume, sei, label])

print(f"Generated {n_samples} records at: {output_file}")
print(f"Class distribution: Fail (0): {class_counts[0]}, Marginal (1): {class_counts[1]}, Pass (2): {class_counts[2]}")