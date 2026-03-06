"""
Mock packet_handler module for testing.
This allows tests to import client/server modules without the actual packet_handler.
"""

import sys
import types

# Create a mock packet_handler module
mock_packet_handler = types.ModuleType('packet_handler')
mock_packet_handler_module = types.ModuleType('packet_handler.packet_handler')

# Mock functions that are imported
async def mock_send_packet(writer, packet, reader=None, logger=None):
    """Mock send_packet function."""
    pass

async def mock_receive_packet(reader, writer=None, logger=None):
    """Mock receive_packet function."""
    return {'packet_type': 'no_tasks'}

# Add functions to the mock module
mock_packet_handler_module.send_packet = mock_send_packet
mock_packet_handler_module.receive_packet = mock_receive_packet

# Register the modules
sys.modules['packet_handler'] = mock_packet_handler
sys.modules['packet_handler.packet_handler'] = mock_packet_handler_module
