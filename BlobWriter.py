# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project root for
# full license information.

import time
import os, uuid, sys
import sys
import asyncio
from six.moves import input
import threading
from azure.iot.device.aio import IoTHubModuleClient
from azure.storage.blob import BlockBlobService, PublicAccess



# Connect to Local Azure Storage
def blobstore():
    try:
        block_blob_service = BlockBlobService(connection_string='DefaultEndpointsProtocol=http;AccountName=localstorageacct;AccountKey=<ACCTKEY>;BlobEndpoint=http://blobstoremodule:11002/localstorageacct;')
    
        block_blob_service._X_MS_VERSION = '2017-04-17'

        container_name ='rootcontainer'
        #block_blob_service.create_container(container_name)
        # Create a file in Documents to test the upload and download.
        #local_path=os.path.expanduser("~/Documents")

        local_file_name = str(uuid.uuid4()) + ".txt"
        local_file_name_with_path = os.path.realpath("/output/") + local_file_name

        print("local_file_name_with_path: ", local_file_name_with_path)

        f = open(str(local_file_name_with_path),"wb")
        f.seek(136314880-1)
        f.write(b"\0")
        f.close()
        print("File Size:", str(os.stat(local_file_name_with_path)))

        block_blob_service.create_blob_from_path(container_name,local_file_name,local_file_name_with_path)
        print("File transfer complete")
    except Exception as e:
        print(e)


async def main():
    try:
        if not sys.version >= "3.5.3":
            raise Exception( "The sample requires python 3.5.3+. Current version of Python: %s" % sys.version )
        print ( "IoT Hub Client for Python" )

        # The client object is used to interact with your Azure IoT hub.
        module_client = IoTHubModuleClient.create_from_edge_environment()

        # connect the client.
        await module_client.connect()

        print ( "Connecting to Blobstore... ")

        i = 0
        while i <= 100:
            print ("Iteration:", i)
            blobstore()
            i = i +1
            time.sleep(180)
        


        # define behavior for receiving an input message on input1
        async def input1_listener(module_client):
            while True:
                input_message = await module_client.receive_message_on_input("input1")  # blocking call
                print("the data in the message received on input1 was ")
                print(input_message.data)
                print("custom properties are")
                print(input_message.custom_properties)
                print("forwarding mesage to output1")
                await module_client.send_message_to_output(input_message, "output1")

        # define behavior for halting the application
        def stdin_listener():
            while True:
                try:
                    selection = input("Press Q to quit\n")
                    if selection == "Q" or selection == "q":
                        print("Quitting...")
                        break
                except:
                    time.sleep(10)

        # Schedule task for C2D Listener
        listeners = asyncio.gather(input1_listener(module_client))



        print ( "The sample is now waiting for messages. ")



        # Run the stdin listener in the event loop
        loop = asyncio.get_event_loop()
        user_finished = loop.run_in_executor(None, stdin_listener)

        # Wait for user to indicate they are done listening for messages
        await user_finished

        # Cancel listening
        listeners.cancel()

        # Finally, disconnect
        await module_client.disconnect()

    except Exception as e:
        print ( "Unexpected error %s " % e )
        raise

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.close()

    # If using Python 3.7 or above, you can use following code instead:
    # asyncio.run(main())
