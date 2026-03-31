import asyncio
import pytest
import json
from unittest.mock import AsyncMock, patch, MagicMock
from network.market_data_hub import MarketDataHub

@pytest.fixture
def anyio_backend():
    return 'asyncio'

@pytest.mark.anyio
async def test_singleton_per_url():
    # Clear instances for testing
    MarketDataHub._instances.clear()
    
    hub1 = await MarketDataHub.get_instance("ws://test1")
    hub2 = await MarketDataHub.get_instance("ws://test1")
    hub3 = await MarketDataHub.get_instance("ws://test2")
    
    assert hub1 is hub2
    assert hub1 is not hub3

@pytest.mark.anyio
async def test_subscribe_unsubscribe():
    MarketDataHub._instances.clear()
    hub = await MarketDataHub.get_instance("ws://test_sub")
    
    callback1 = AsyncMock()
    callback2 = AsyncMock()
    
    # Mock the internal client and its listen method
    mock_client = MagicMock()
    mock_client.listen = AsyncMock()
    
    with patch('network.market_data_hub.WebSocketClient', return_value=mock_client):
        await hub.subscribe(callback1)
        await hub.subscribe(callback2)
        
        assert len(hub.subscribers) == 2
        
        await hub.unsubscribe(callback1)
        assert len(hub.subscribers) == 1
        
        await hub.unsubscribe(callback2)
        assert len(hub.subscribers) == 0

@pytest.mark.anyio
async def test_broadcasting():
    MarketDataHub._instances.clear()
    hub = await MarketDataHub.get_instance("ws://test_broadcast")
    
    callback1 = AsyncMock()
    callback2 = AsyncMock()
    
    # Manually add subscribers
    hub.subscribers.add(callback1)
    hub.subscribers.add(callback2)
    
    test_msg = json.dumps({"test": "data"})
    await hub._distribute(test_msg)
    
    # Give time for tasks to finish
    await asyncio.sleep(0.1)
    
    callback1.assert_called_once_with(test_msg)
    callback2.assert_called_once_with(test_msg)

@pytest.mark.anyio
async def test_connection_lifecycle():
    MarketDataHub._instances.clear()
    url = "ws://test_lifecycle"
    hub = await MarketDataHub.get_instance(url)
    
    mock_client = MagicMock()
    mock_client.listen = AsyncMock()
    hub.client = mock_client
    
    # First subscription should start the client
    # Using a dummy wrapper because subscribe calls create_task
    with patch('network.market_data_hub.asyncio.create_task') as mock_create_task:
        await hub.subscribe(AsyncMock())
        assert hub.is_listening is True
        mock_create_task.assert_called_once()
        
        # Second subscription should NOT start a new task
        await hub.subscribe(AsyncMock())
        assert mock_create_task.call_count == 1
