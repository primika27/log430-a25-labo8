"""
Handler: Payment Created
SPDX-License-Identifier: LGPL-3.0-or-later
Auteurs : Gabriel C. Ullmann, Fabio Petrillo, 2025
"""
from typing import Dict, Any
import config
from event_management.base_handler import EventHandler
from orders.commands.order_event_producer import OrderEventProducer

class PaymentCreatedHandler(EventHandler):
    """Handles PaymentCreated events"""
    
    def __init__(self):
        self.order_producer = OrderEventProducer()
        super().__init__()
    
    def get_event_type(self) -> str:
        """Get event type name"""
        return "PaymentCreated"
    
    def handle(self, event_data: Dict[str, Any]) -> None:
        """Execute every time the event is published"""
        # Selon le diagramme: PaymentCreated (état 3) -> COMPLETING_ORDER_SAGA (état 4)
        # Le paiement a été créé avec succès, la saga est terminée
        # Note: Le payment_link est déjà ajouté par OutboxProcessor
        
        self.logger.info(f"Paiement créé avec succès pour order_id={event_data.get('order_id')}")
        self.logger.debug(f"payment_link={event_data.get('payment_link')}")
        
        # Terminer la saga avec succès
        event_data['event'] = "SagaCompleted"
        OrderEventProducer().get_instance().send(config.KAFKA_TOPIC, value=event_data)


