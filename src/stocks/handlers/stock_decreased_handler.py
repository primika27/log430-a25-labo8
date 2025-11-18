"""
Handler: Stock Decreased
SPDX-License-Identifier: LGPL-3.0-or-later
Auteurs : Gabriel C. Ullmann, Fabio Petrillo, 2025
"""
from typing import Dict, Any
import config
from db import get_sqlalchemy_session
from event_management.base_handler import EventHandler
from orders.commands.order_event_producer import OrderEventProducer
from payments.models.outbox import Outbox
from payments.outbox_processor import OutboxProcessor


class StockDecreasedHandler(EventHandler):
    """Handles StockDecreased events"""
    
    def __init__(self):
        self.order_producer = OrderEventProducer()
        super().__init__()
    
    def get_event_type(self) -> str:
        """Get event type name"""
        return "StockDecreased"
    
    def handle(self, event_data: Dict[str, Any]) -> None:
        """Execute every time the event is published"""
        # Selon le diagramme: StockDecreased (état 2) -> CREATING_PAYMENT (état 3)
        # Le stock a été diminué avec succès, maintenant on doit créer le paiement
        session = get_sqlalchemy_session()
        
        try:
            # Créer un enregistrement dans la table Outbox pour garantir la tolérance aux pannes
            new_outbox_item = Outbox(
                order_id=event_data['order_id'],
                user_id=event_data['user_id'],
                total_amount=event_data['total_amount'],
                order_items=event_data['order_items']
            )
            session.add(new_outbox_item)
            session.flush()
            session.commit()
            
            self.logger.debug(f"Enregistrement Outbox créé pour order_id={event_data['order_id']}")
            
            # Lancer le traitement du paiement via OutboxProcessor
            OutboxProcessor().run(new_outbox_item)
            
            # Note: OutboxProcessor enverra soit PaymentCreated soit PaymentCreationFailed
            # donc nous ne publions pas d'événement ici
            
        except Exception as e:
            session.rollback()
            self.logger.debug("La création d'une transaction de paiement a échoué : " + str(e))
            event_data['event'] = "PaymentCreationFailed"
            event_data['error'] = str(e)
            OrderEventProducer().get_instance().send(config.KAFKA_TOPIC, value=event_data)
        finally:
            session.close()


