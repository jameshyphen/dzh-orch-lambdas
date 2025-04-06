class ProductStateUpdateEvent:
    def __init__(self, eventId, eventType, source, metadata):
        self.eventId = eventId
        self.eventType = eventType
        self.source = source
        self.metadata = metadata

    def to_dict(self):
        return {
            "eventId": self.eventId,
            "eventType": self.eventType,
            "metadata": self.metadata
        }
