"""
Handler: Stock Decrease Failed
SPDX-License-Identifier: LGPL-3.0-or-later
Auteurs : Gabriel C. Ullmann, Fabio Petrillo, 2025
"""
from typing import Dict, Any
import config
from event_management.base_handler import EventHandler
from orders.commands.order_event_producer import OrderEventProducer


class StockDecreaseFailedHandler(EventHandler):
    """Handles StockDecreaseFailed events"""
    
    def __init__(self):
        self.order_producer = OrderEventProducer()
        super().__init__()
    
    def get_event_type(self) -> str:
        """Get event type name"""
        return "StockDecreaseFailed"
    
    def handle(self, event_data: Dict[str, Any]) -> None:
        """Execute every time the event is published"""
        # Selon le diagramme: StockDecreaseFailed (état 2) -> CANCELLING_ORDER (état 6)
        # La diminution du stock a échoué, il faut annuler la commande directement
        
        self.logger.error(f"Échec de la diminution du stock pour order_id={event_data.get('order_id')}: {event_data.get('error')}")
        
        # Pas de compensation nécessaire car le stock n'a jamais été modifié
        # On passe directement à l'annulation de la commande
        event_data['event'] = "OrderCancelled"
        OrderEventProducer().get_instance().send(config.KAFKA_TOPIC, value=event_data)
  
