import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import folium

from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.metrics import mean_squared_error, mean_absolute_percentage_error, r2_score

import xgboost as xgb

# Assuming gcsfs is installed
gcs_uri = ''
df = pd.read_csv(gcs_uri)

# Feature Engineering
df['interaction_xy'] = df['x'] * df['y']  # Example of an interaction term
df['xAcceleration_squared'] = df['xAcceleration'] ** 2  # Non-linear transformation

# Calculate the total velocity magnitude
df['velocity_magnitude'] = np.sqrt(df['xVelocity']**2 + df['yVelocity']**2)

# ML Preparation
features = ['x', 'y', 'xAcceleration', 'interaction_xy', 'xAcceleration_squared'] + ['laneId']
X = df[features]
y = df['velocity_magnitude']

# Convert 'laneId' into categorical variables and include in features
X = pd.get_dummies(X, columns=['laneId'], drop_first=True)

# Splitting the dataset
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Initialize XGBoost Regressor
model = xgb.XGBRegressor(objective ='reg:squarederror')

# Hyperparameter tuning
params = {
    'colsample_bytree': [0.3, 0.7],
    'learning_rate': [0.01, 0.1],
    'max_depth': [5, 10],
    'alpha': [1, 10],
    'n_estimators': [100, 200]
}

grid_search = GridSearchCV(model, param_grid=params, cv=3, scoring='neg_mean_squared_error', verbose=1)
grid_search.fit(X_train, y_train)

# Best model
best_model = grid_search.best_estimator_

# Predictions
y_pred = best_model.predict(X_test)

# Evaluation
rmse = np.sqrt(mean_squared_error(y_test, y_pred))
mape = mean_absolute_percentage_error(y_test, y_pred) * 100
r2 = r2_score(y_test, y_pred)

print(f"Root Mean Squared Error: {rmse}")
print(f"Mean Absolute Percentage Error (MAPE): {mape:.2f}%")
print(f"R-squared: {r2:.2f}")

# Feature Importance
feature_importances = pd.Series(best_model.feature_importances_, index=X.columns)
feature_importances.nlargest(10).plot(kind='barh')
plt.title('Top 10 Feature Importances')
plt.show()

# Actual vs Predicted Scatter Plot
plt.scatter(y_test, y_pred, alpha=0.3)
plt.plot([y.min(), y.max()], [y.min(), y.max()], 'k--', lw=2)  # Line of perfect prediction
plt.xlabel('Actual Velocities')
plt.ylabel('Predicted Velocities')
plt.title('Actual vs Predicted Velocities')
plt.show()

# Example of new data (replace this with your actual new data)
gcs_uris = ''
df_new = pd.read_csv(gcs_uris)

# Apply the same feature engineering to the new data
df_new['interaction_xy'] = df_new['x'] * df_new['y']
df_new['xAcceleration_squared'] = df_new['xAcceleration'] ** 2

# Select the same features used for training
features_new = df_new[features]  # 'features' list should be the same as used for model training

# Convert 'laneId' into categorical variables and include in features, as before
X_new = pd.get_dummies(features_new, columns=['laneId'], drop_first=True)

# Ensure the new data has the same feature columns as the model expects
# This is a simplistic approach; in practice, you might need to check and adjust the columns

# Predict velocity magnitude for the new data
predicted_velocity = best_model.predict(X_new)

# Display the predictions
print("Predicted velocity magnitudes:", predicted_velocity)

import matplotlib.pyplot as plt
import matplotlib.colors as mcolors

# Ensure you have a figure and axis to plot onto
fig, ax = plt.subplots(figsize=(10, 6))

# Create a scatter plot where the color intensity represents the velocity magnitude
# Normalize the velocity magnitudes to fit between 0 and 1 for coloring
norm = mcolors.Normalize(vmin=min(predicted_velocity), vmax=max(predicted_velocity))

# Plot each point, with the color intensity based on its predicted velocity
scatter = ax.scatter(df_new['x'], df_new['y'], c=predicted_velocity, cmap='viridis', norm=norm)

# Create a colorbar to represent the velocity magnitudes
cbar = plt.colorbar(scatter, ax=ax)
cbar.set_label('Predicted Velocity Magnitude (m/s)')

# Set the titles and labels
ax.set_title('Predicted Traffic Velocities')
ax.set_xlabel('X Coordinate')
ax.set_ylabel('Y Coordinate')

# Display the plot
plt.show()

from google.cloud import bigquery

# Construct a BigQuery client object.
client = bigquery.Client()

df_new['predicted_velocity'] = predicted_velocity

# Now `df_new` includes the `predicted_velocity` column, so we can proceed to define the schema
# Define schema here only if it's necessary. BigQuery can auto-detect schema if not provided.
job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_TRUNCATE",  # Replace the table data
    autodetect=True,  # Auto-detects the schema
)

# You don't need to specify the schema in LoadJobConfig if you're using autodetect
job = client.load_table_from_dataframe(df_new, table_id, job_config=job_config)

# Wait for the load job to complete.
job.result()

print("The data has been uploaded to BigQuery")
