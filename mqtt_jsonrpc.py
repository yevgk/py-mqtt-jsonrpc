import asyncio
import json
import uuid
import re
from asyncio_mqtt import Client, MqttError
from contextlib import AsyncExitStack
from jsonrpcclient import Ok, parse_json, request
# from jsonrpcserver import async_dispatch

class MqttJsonRpcService:

    def __init__(self, mqtt_host, reconnect_interval = 3, async_dispatch = None,connected_co = None, diconnected_co = None):
        self.mqtt_host = mqtt_host
        self.reconnect_interval = reconnect_interval
        self.async_dispatch = async_dispatch
        self.connected_co = connected_co
        self.diconnected_co = diconnected_co
        self.connected = False
        self.rpc_response_fut = {}


    async def rpc(self, method, params):
        req = request(method, params)
        loop = asyncio.get_running_loop() # Get the current event loop
        fut = loop.create_future() # Create a new Future object for the response
        self.rpc_response_fut[req["id"]] = fut
        await self.client.publish("jsonrpc/%s/request" % self.client_id, json.dumps(req), qos=2)
        return await fut


    async def publish(self, *args, **kw):
        return await self.client.publish(*args, **kw)


    async def subscribe(self, topic, handler_coro=None):
        if handler_coro:
            manager = self.client.filtered_messages(topic)
            messages = await self.stack.enter_async_context(manager)
            task = asyncio.create_task(handler_coro(self, messages))
            self.tasks.add(task)

        return await self.client.subscribe(topic)


    async def handle_jsonrpc_request(self, messages):
        async for message in messages:
            # Note that we assume that the message paylod is an UTF8-encoded string (hence the `bytes.decode` call)
            msg = message.payload.decode()
            print(f'[JSON-RPC topic="{message.topic}"] {msg}')
            regexpCall = re.compile('jsonrpc/(.+)/request')
            matchCall = regexpCall.search(message.topic)
            if matchCall:
                rpc_client_id = matchCall.group(1)
                if response := await self.async_dispatch(msg):
                    await self.client.publish("jsonrpc/%s/response" % rpc_client_id, response, qos=2)


    async def handle_jsonrpc_response(self, messages):
        async for message in messages:
            # Note that we assume that the message paylod is an UTF8-encoded string (hence the `bytes.decode` call)
            msg = message.payload.decode()
            print(f'[JSON-RPC topic="{message.topic}"] {msg}')
            response = parse_json(msg)
            # print(response)
            if response.id in self.rpc_response_fut:
                self.rpc_response_fut[response.id].set_result(response)


    async def mqtt_service(self):
        # Let's create a stack to help manage context managers.
        async with AsyncExitStack() as stack:
            # Keep track of the asyncio tasks that we create, so that we can cancel them on exit
            self.tasks = set()
            stack.push_async_callback(self.cancel_tasks, self.tasks)

            self.stack = stack

            self.client_id = uuid.uuid4().hex

            # Connect to the MQTT broker
            client = Client(self.mqtt_host)
            self.client = client
            await stack.enter_async_context(client)

            self.connected = True

            manager = client.filtered_messages("jsonrpc/%s/response" % self.client_id)
            messages = await stack.enter_async_context(manager)
            task = asyncio.create_task(self.handle_jsonrpc_response(messages))
            self.tasks.add(task)

            if self.async_dispatch:
                manager = client.filtered_messages("jsonrpc/+/request")
                messages = await stack.enter_async_context(manager)
                task = asyncio.create_task(self.handle_jsonrpc_request(messages))
                self.tasks.add(task)

            # Messages that doesn't match a filter will get logged here
            messages = await stack.enter_async_context(client.unfiltered_messages())
            task = asyncio.create_task(self.log_messages(messages, '[topic="{topic}"] {payload}'))
            self.tasks.add(task)

            # Subscribe to topic(s)
            # Note that we subscribe *after* starting the message loggers. Otherwise, we may miss retained messages.
            await client.subscribe("jsonrpc/#")

            if self.connected_co:
                await self.connected_co(self)

            # Wait for everything to complete (or fail due to, e.g., network errors)
            await asyncio.gather(*self.tasks)

            self.connected = False


    async def log_messages(self, messages, template):
        async for message in messages:
            # Note that we assume that the message paylod is an UTF8-encoded string (hence the `bytes.decode` call).
            print(template.format(topic = message.topic, payload = message.payload.decode()))


    async def cancel_tasks(self, tasks):
        for task in tasks:
            if task.done():
                continue
            try:
                task.cancel()
                await task
            except asyncio.CancelledError:
                pass


    async def run(self):
        # Run the advanced_example indefinitely. Reconnect automatically if the connection is lost
        while True:
            try:
                await self.mqtt_service()
            except MqttError as error:
                print(f'Error "{error}". Reconnecting in {self.reconnect_interval} seconds.')
            finally:
                await asyncio.sleep(self.reconnect_interval)
