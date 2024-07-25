import mqtt from "mqtt";
import io from "socket.io-client";
import dotenv from "dotenv";
import axios from "axios";

dotenv.config();

const brokerAddress: string = process.env.MQTT_BROKER_URL || "mqtt://localhost:1883";
const topicData: string = process.env.TOPIC_DATA || "data";
const secret = process.env.SECRET_KEY;

const socket = io(process.env.SOCKET_SERVER_URL ?? "", {
  auth: {
    token: `${secret}`,
  },
  transports: ["websocket"],
});

const sendIncomingData = (data: { topic: string; message: string }) => {
  socket.emit("IncomingData", data);
};

let client: mqtt.MqttClient;

try {
  client = mqtt.connect(brokerAddress);
} catch (error: any) {
  console.error("Error connecting to MQTT broker:", error.message);
}

client = mqtt.connect(brokerAddress);

const lastSavedTime: { [sensorId: number]: number } = {};
const saveInterval = 5 * 60 * 1000; // 5 minutos

const url = process.env.BACKEND_URL || ""  

const saveSensorData = async (sensorData: any) => {
  for (const sensor of sensorData.sensors) {
    const now = Date.now();

    // Lógica de guardado condicional
    if (
      sensor.dataType === "movimiento" && sensor.data ||
      sensor.dataType === "fuego" && sensor.data ||
      !lastSavedTime[sensor.sensorId] ||
      now - lastSavedTime[sensor.sensorId] >= saveInterval
    ) {
      try {
        // Crear el registro de datos del sensor
        const response = await axios.post(`${url}/sensor-data`, {
          sensorId: sensor.sensorId,
          dataType: sensor.dataType,
          data: JSON.stringify(sensor.data)
        });

        console.log(`Sensor data saved: ${response.data}`);

        // Asociar el sensor al usuario
        await axios.post(`${url}/user-sensors`, {
          user_id: sensorData.userId,
          sensor_data_id: response.data.id,
          location: sensor.location || 'Unknown'
        });

        console.log(`Sensor associated with user: ${sensorData.userId}`);

        // Actualizar el último tiempo de guardado
        lastSavedTime[sensor.sensorId] = now;
      } catch (error) {
        console.error('Error processing sensor data:', error);
      }
    }
  }
};

if (client) {
  client.on("error", (err) => {
    console.error("Connection error:", err);
  });

  client.on("connect", () => {
    console.log("Connected to MQTT broker");

    client.subscribe(topicData, { qos: 1 }, (err) => {
      if (err) {
        console.error("Error subscribing to topic:", err);
      } else {
        console.log(`Subscribed to topic: ${topicData}`);
      }
    });
  });

  client.on("message", async (topic, message) => {
    console.log(`Received message from topic: ${topic}`);
    console.log(`Message: ${message.toString()}`);

    sendIncomingData({
      topic: topic,
      message: message.toString(),
    });

    try {
      const sensorData = JSON.parse(message.toString());
      await saveSensorData(sensorData);
    } catch (error) {
      console.error('Error parsing or saving sensor data:', error);
    }
  });
}
