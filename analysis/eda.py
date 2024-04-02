import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

gcs_uri = ''

df = pd.read_csv(gcs_uri)

# Calculate the total velocity magnitude
df['velocity_magnitude'] = np.sqrt(df['xVelocity']**2 + df['yVelocity']**2)

# Print basic info about the dataset
print(df.info())

# Print summary statistics for numerical features
print(df.describe())

# Plot the distribution of total velocity magnitudes
plt.figure(figsize=(10, 6))
plt.hist(df['velocity_magnitude'], bins=50, color='blue', edgecolor='black')
plt.title('Distribution of Total Velocity Magnitudes')
plt.xlabel('Velocity Magnitude (m/s)')
plt.ylabel('Count')
plt.show()

# Plot lane usage
lane_usage = df['laneId'].value_counts().sort_index()
plt.figure(figsize=(10, 6))
lane_usage.plot(kind='bar', color='green')
plt.title('Lane Usage')
plt.xlabel('Lane ID')
plt.ylabel('Number of Vehicles')
plt.xticks(rotation=0)
plt.show()

# Plot average acceleration by lane, assuming 'xAcceleration' represents the longitudinal acceleration
avg_acceleration_by_lane = df.groupby('laneId')['xAcceleration'].mean()
plt.figure(figsize=(10, 6))
avg_acceleration_by_lane.plot(kind='bar', color='orange')
plt.title('Average Longitudinal Acceleration by Lane')
plt.xlabel('Lane ID')
plt.ylabel('Average Acceleration (m/s^2)')
plt.xticks(rotation=0)
plt.show()

# Visualizing position data for a sample of vehicles
sample_vehicle_ids = df['id'].drop_duplicates().sample(n=5)
for vehicle_id in sample_vehicle_ids:
    vehicle_data = df[df['id'] == vehicle_id]
    plt.plot(vehicle_data['x'], vehicle_data['y'], label=f'Vehicle {vehicle_id}')
plt.legend()
plt.title('Trajectories of Sampled Vehicles')
plt.xlabel('X Position (m)')
plt.ylabel('Y Position (m)')
plt.show()
