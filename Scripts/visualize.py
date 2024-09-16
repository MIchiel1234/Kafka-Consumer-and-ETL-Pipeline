import matplotlib.pyplot as plt
import pandas as pd

# Read the transformed data
df = pd.read_csv('/tmp/transformed_kafka_data.csv')

# Plot the data
df.plot(x='timestamp', y='value', kind='line')
plt.title('Kafka Data Visualization')
plt.xlabel('Timestamp')
plt.ylabel('Value')
plt.show()
