"""
Handler: Stock Increased
SPDX-License-Identifier: LGPL-3.0-or-later
Auteurs : Gabriel C. Ullmann, Fabio Petrillo, 2025
"""
from typing import Dict, Any
import config
from db import get_sqlalchemy_session
from event_management.base_handler import EventHandler
from orders.commands.order_event_producer import OrderEventProducer
from stocks.commands.write_stock import check_in_items_to_stock


class StockIncreasedHandler(EventHandler):
    """Handles StockIncreased events"""
    
    def __init__(self):
        self.order_producer = OrderEventProducer()
        super().__init__()
    
    def get_event_type(self) -> str:
        """Get event type name"""
        return "StockIncreased"
    
    def handle(self, event_data: Dict[str, Any]) -> None:
        """Execute every time the event is published"""
        # Selon le diagramme: StockIncreased (état 5) -> CANCELLING_ORDER (état 6)
        # Le paiement a échoué, on a compensé en réaugmentant le stock
        # Maintenant on doit annuler la commande
        
        session = get_sqlalchemy_session()
        
        try:
            # Compenser en réaugmentant le stock (qui avait été diminué précédemment)
            check_in_items_to_stock(session, event_data['order_items'])
            session.commit()
            
            self.logger.debug(f"Stock compensé (réaugmenté) pour order_id={event_data.get('order_id')}")
            
            # Maintenant que le stock est compensé, annuler la commande
            event_data['event'] = "OrderCancelled"
            OrderEventProducer().get_instance().send(config.KAFKA_TOPIC, value=event_data)
            
        except Exception as e:
            session.rollback()
            self.logger.error(f"Erreur lors de la compensation du stock: {e}")
            # Même en cas d'erreur de compensation, on annule la commande
            event_data['event'] = "OrderCancelled"
            event_data['error'] = f"Stock compensation failed: {str(e)}"
            OrderEventProducer().get_instance().send(config.KAFKA_TOPIC, value=event_data)
        finally:
            session.close()



