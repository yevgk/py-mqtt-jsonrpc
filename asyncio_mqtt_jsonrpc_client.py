import asyncio
from mqtt_jsonrpc import MqttJsonRpcService
from random import randrange
import json
import os

MQTT_HOST = "127.0.0.1"


async def monitor(service):
    while True:
        print(".")
        await asyncio.sleep(2)
        if service.connected:
            print(await service.rpc("helloWorld", { "a": 12345, "b": "qwerty" }))
            await asyncio.sleep(1)
            await service.publish("telemetry/test_client", json.dumps({ "rnd": randrange(100) }), qos=0)


async def telemetry(service, messages):
    async for message in messages:
        # Note that we assume that the message paylod is an UTF8-encoded string (hence the `bytes.decode` call)
        print('[telemetry topic="{topic}"] {payload}'.format(topic = message.topic, payload = message.payload.decode()))


async def connected(service):
    print("Service connected!")
    await service.subscribe("telemetry/#", telemetry)
    # await service.subscribe("telemetry/#")

    print(await service.rpc("validValue", {}))


async def disconnected(service):
    print("Service disconnected!")


async def main():
    service = MqttJsonRpcService(MQTT_HOST, connected_co=connected, diconnected_co=disconnected)
    await asyncio.gather(service.run(), monitor(service))


if __name__ == "__main__":
    if os.name == 'nt':
        # Change to the "Selector" event loop
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    asyncio.run(main())
