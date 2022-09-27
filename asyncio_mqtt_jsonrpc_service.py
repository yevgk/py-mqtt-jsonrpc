import asyncio
from mqtt_jsonrpc import MqttJsonRpcService
import os
from random import randrange
import json

from jsonrpcserver import method, Success, Result, Error, InvalidParams, async_dispatch

@method
async def helloWorld(**kwargs) -> Result:
    return Success({ "result": "Hello World!", "kwargs": kwargs })

@method
async def validValue(x) -> Result:
    if x == "":
        return InvalidParams("Null value")
    result = { "x": x, "result" : "Ok" }
    return Success(result)



async def telemetry(service, messages):
    async for message in messages:
        # Note that we assume that the message paylod is an UTF8-encoded string (hence the `bytes.decode` call)
        print('[topic="{topic}"] {payload}'.format(topic = message.topic, payload = message.payload.decode()))


async def connected(service):
    print("Service connected!")
    # await service.subscribe("telemetry/#", telemetry)
    await service.subscribe("telemetry/#")


async def disconnected(service):
    print("Service disconnected!")


async def monitor(service):
    while True:
        if service.connected:
            await service.publish("telemetry/test", json.dumps({ "rnd": randrange(100) }), qos=0)
        await asyncio.sleep(2)



MQTT_HOST = "127.0.0.1"

async def main():
    service = MqttJsonRpcService(MQTT_HOST, async_dispatch=async_dispatch, connected_co=connected, diconnected_co=disconnected)
    await asyncio.gather(service.run(), monitor(service))


if __name__ == "__main__":
    if os.name == 'nt':
        # Change to the "Selector" event loop
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    asyncio.run(main())
