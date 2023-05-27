import asyncio

from aio_pika import DeliveryMode, Message, connect_robust

RABBITMQ_CONFIG = {
    "username": "guest",
    "password": "guest",
    "host": "127.0.0.1",
    "port": 5672
}


async def product(queuename, message_body) -> None:
    connection = await connect_robust(
        "amqp://guest:guest@127.0.0.1/"
    )
    async with connection:
        # Creating a channel
        channel = await connection.channel()

        message = Message(
            message_body, delivery_mode=DeliveryMode.PERSISTENT,
        )

        # Sending the message
        await channel.default_exchange.publish(
            message, routing_key=queuename,
        )

        print(f" [x] Sent {message!r}")


if __name__ == "__main__":
    data = [
        """<212>Oct 29 11:50:51 ips IPS: SerialNum=0113201403099999 GenTime="2014-10-29 11:50:51" SrcIP=192.168.38.28 SrcIP6= SrcIPVer=4 DstIP=82.244.22.168 DstIP6= DstIPVer=4 Protocol=UDP SrcPort=58380 DstPort=16464 InInterface=ge0/3 OutInterface=ge0/4 SMAC=00:0c:29:bb:28:60 DMAC=00:17:df:ba:4c:00 FwPolicyID=6 EventName=UDP_Trojan.Malagent_连接 EventID=152340118 EventLevel=2 EventsetName=all SecurityType=木马后门 SecurityID=5 ProtocolType=UDP ProtocolID=5 Action=PASS Vsysid=0 Content="" CapToken= EvtCount=1""",
        """<213>Feb 23 11:34:42 ips IPS: SerialNum=0113251411269999 GenTime="2016-02-23 11:34:42" SrcIP=192.168.13.124 SrcIP6= SrcIPVer=4 DstIP=192.168.13.1 DstIP6= DstIPVer=4 Protocol=UDP SrcPort=161 DstPort=53457 InInterface=ge0/1 OutInterface=ge0/2 SMAC=00:10:f3:48:cd:28 DMAC=00:10:f3:fa:0d:80 FwPolicyID=6 EventName=SNMP_缺省口令[public] EventID=152518449 EventLevel=1 EventsetName=ALL_test SecurityType=可疑行为 SecurityID=12 ProtocolType=SNMP ProtocolID=27 Action=PASS Vsysid=0 Content="口令=public;" CapToken=308890 EvtCount=1""",
        """<213>Feb 23 11:34:42 ips IPS: SerialNum=0113251411269999 GenTime="2016-02-23 11:34:42" SrcIP=192.168.13.124 SrcIP6= SrcIPVer=4 DstIP=192.168.13.1 DstIP6= DstIPVer=4 Protocol=UDP SrcPort=161 DstPort=53457 InInterface=ge0/1 OutInterface=ge0/2 SMAC=00:10:f3:48:cd:28 DMAC=00:10:f3:fa:0d:80 FwPolicyID=6 EventName=SNMP_缺省口令[public] EventID=152518449 EventLevel=1 EventsetName=ALL_test SecurityType=可疑行为 SecurityID=12 ProtocolType=SNMP ProtocolID=27 Action=PASS Vsysid=0 Content="口令=public;" CapToken=308890 EvtCount=1""",
        """<213>Feb 23 11:34:42 ips IPS: SerialNum=0113251411269999 GenTime="2016-02-23 11:34:42" SrcIP=192.168.13.124 SrcIP6= SrcIPVer=4 DstIP=192.168.13.1 DstIP6= DstIPVer=4 Protocol=UDP SrcPort=161 DstPort=53457 InInterface=ge0/1 OutInterface=ge0/2 SMAC=00:10:f3:48:cd:28 DMAC=00:10:f3:fa:0d:80 FwPolicyID=6 EventName=SNMP_缺省口令[public] EventID=152518449 EventLevel=1 EventsetName=ALL_test SecurityType=可疑行为 SecurityID=12 ProtocolType=SNMP ProtocolID=27 Action=PASS Vsysid=0 Content="口令=public;" CapToken=308890 EvtCount=1""",
        """<213>Feb 23 11:34:42 ips IPS: SerialNum=0113251411269999 GenTime="2016-02-23 11:34:42" SrcIP=192.168.13.124 SrcIP6= SrcIPVer=4 DstIP=192.168.13.1 DstIP6= DstIPVer=4 Protocol=UDP SrcPort=161 DstPort=53457 InInterface=ge0/1 OutInterface=ge0/2 SMAC=00:10:f3:48:cd:28 DMAC=00:10:f3:fa:0d:80 FwPolicyID=6 EventName=SNMP_缺省口令[public] EventID=152518449 EventLevel=1 EventsetName=ALL_test SecurityType=可疑行为 SecurityID=12 ProtocolType=SNMP ProtocolID=27 Action=PASS Vsysid=0 Content="口令=public;" CapToken=308890 EvtCount=1""",
        """<213>Feb 23 11:34:42 ips IPS: SerialNum=0113251411269999 GenTime="2016-02-23 11:34:42" SrcIP=192.168.13.124 SrcIP6= SrcIPVer=4 DstIP=192.168.13.1 DstIP6= DstIPVer=4 Protocol=UDP SrcPort=161 DstPort=53457 InInterface=ge0/1 OutInterface=ge0/2 SMAC=00:10:f3:48:cd:28 DMAC=00:10:f3:fa:0d:80 FwPolicyID=6 EventName=SNMP_缺省口令[public] EventID=152518449 EventLevel=1 EventsetName=ALL_test SecurityType=可疑行为 SecurityID=12 ProtocolType=SNMP ProtocolID=27 Action=PASS Vsysid=0 Content="口令=public;" CapToken=308890 EvtCount=1""",
        """<213>Feb 23 11:34:42 ips IPS: SerialNum=0113251411269999 GenTime="2016-02-23 11:34:42" SrcIP=192.168.13.124 SrcIP6= SrcIPVer=4 DstIP=192.168.13.1 DstIP6= DstIPVer=4 Protocol=UDP SrcPort=161 DstPort=53457 InInterface=ge0/1 OutInterface=ge0/2 SMAC=00:10:f3:48:cd:28 DMAC=00:10:f3:fa:0d:80 FwPolicyID=6 EventName=SNMP_缺省口令[public] EventID=152518449 EventLevel=1 EventsetName=ALL_test SecurityType=可疑行为 SecurityID=12 ProtocolType=SNMP ProtocolID=27 Action=PASS Vsysid=0 Content="口令=public;" CapToken=308890 EvtCount=1""",
        """<213>Feb 23 11:34:42 ips IPS: SerialNum=0113251411269999 GenTime="2016-02-23 11:34:42" SrcIP=192.168.13.124 SrcIP6= SrcIPVer=4 DstIP=192.168.13.1 DstIP6= DstIPVer=4 Protocol=UDP SrcPort=161 DstPort=53457 InInterface=ge0/1 OutInterface=ge0/2 SMAC=00:10:f3:48:cd:28 DMAC=00:10:f3:fa:0d:80 FwPolicyID=6 EventName=SNMP_缺省口令[public] EventID=152518449 EventLevel=1 EventsetName=ALL_test SecurityType=可疑行为 SecurityID=12 ProtocolType=SNMP ProtocolID=27 Action=PASS Vsysid=0 Content="口令=public;" CapToken=308890 EvtCount=1""",
        """<213>Feb 23 11:34:42 ips IPS: SerialNum=0113251411269999 GenTime="2016-02-23 11:34:42" SrcIP=192.168.13.124 SrcIP6= SrcIPVer=4 DstIP=192.168.13.1 DstIP6= DstIPVer=4 Protocol=UDP SrcPort=161 DstPort=53457 InInterface=ge0/1 OutInterface=ge0/2 SMAC=00:10:f3:48:cd:28 DMAC=00:10:f3:fa:0d:80 FwPolicyID=6 EventName=SNMP_缺省口令[public] EventID=152518449 EventLevel=1 EventsetName=ALL_test SecurityType=可疑行为 SecurityID=12 ProtocolType=SNMP ProtocolID=27 Action=PASS Vsysid=0 Content="口令=public;" CapToken=308890 EvtCount=1""",
        """<213>Feb 23 11:34:42 ips IPS: SerialNum=0113251411269999 GenTime="2016-02-23 11:34:42" SrcIP=192.168.13.124 SrcIP6= SrcIPVer=4 DstIP=192.168.13.1 DstIP6= DstIPVer=4 Protocol=UDP SrcPort=161 DstPort=53457 InInterface=ge0/1 OutInterface=ge0/2 SMAC=00:10:f3:48:cd:28 DMAC=00:10:f3:fa:0d:80 FwPolicyID=6 EventName=SNMP_缺省口令[public] EventID=152518449 EventLevel=1 EventsetName=ALL_test SecurityType=可疑行为 SecurityID=12 ProtocolType=SNMP ProtocolID=27 Action=PASS Vsysid=0 Content="口令=public;" CapToken=308890 EvtCount=1""",
    ]

    loop = asyncio.get_event_loop()
    max_count = 5
    tasks = [product("raw_data", _.encode()) for _ in data]
    loop.run_until_complete(asyncio.gather(*tasks))
    loop.close()


