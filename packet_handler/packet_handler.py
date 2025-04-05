import asyncio
import json
import struct
import logging

async def send_packet(writer: asyncio.StreamWriter, packet: dict, reader: asyncio.StreamReader, logger: logging.Logger, timeout: float = 10.0):
    """Send a packet prefixed with its size and wait for ACK with timeout."""
    packet_data = json.dumps(packet).encode()
    packet_size = len(packet_data)
    
    size_packet = struct.pack('!Q', packet_size)
    writer.write(size_packet)
    await asyncio.wait_for(writer.drain(), timeout=timeout)
    logger.debug(f"Sent size packet: {packet_size} bytes")
    
    try:
        ack = await asyncio.wait_for(reader.readexactly(1), timeout=timeout)
        if ack != b'A':
            raise ValueError(f"Expected ACK ('A'), got {ack}")
        logger.debug("Received ACK for size packet")
    except asyncio.TimeoutError:
        logger.error(f"Timeout waiting for ACK on packet of size {packet_size}")
        raise
    
    writer.write(packet_data)
    await asyncio.wait_for(writer.drain(), timeout=timeout)
    logger.debug(f"Sent packet of {packet_size} bytes")

async def receive_packet(reader: asyncio.StreamReader, writer: asyncio.StreamWriter, logger: logging.Logger, timeout: float = 10.0) -> dict:
    """Receive a size packet, send ACK, then receive the full packet with timeout."""
    try:
        size_data = await asyncio.wait_for(reader.readexactly(8), timeout=timeout)
        if len(size_data) != 8:
            raise ValueError(f"Expected 8-byte size packet, got {len(size_data)} bytes")
        packet_size = struct.unpack('!Q', size_data)[0]
        logger.debug(f"Received size packet: {packet_size} bytes")
        
        writer.write(b'A')
        await asyncio.wait_for(writer.drain(), timeout=timeout)
        logger.debug("Sent ACK for size packet")
        
        packet_data = await asyncio.wait_for(reader.readexactly(packet_size), timeout=timeout)
        if len(packet_data) != packet_size:
            raise ValueError(f"Expected {packet_size} bytes, got {len(packet_data)}")
        
        return json.loads(packet_data.decode())
    except asyncio.TimeoutError as e:
        logger.error(f"Timeout in receive_packet: {e}")
        raise
    except Exception as e:
        logger.error(f"Error in receive_packet: {e}")
        raise