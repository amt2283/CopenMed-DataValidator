# entre procesador de datos 

import json
import os
from datetime import datetime

class CheckpointManager:
    def __init__(self, path="checkpoint.json"):
        self.path = path
        self.checkpoint = self.load_checkpoint()

    def load_checkpoint(self):
        if os.path.exists(self.path):
            try:
                with open(self.path, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception:
                return self.default_checkpoint()
        return self.default_checkpoint()

    def default_checkpoint(self):
        # Definimos la estructura inicial con todas las claves necesarias.
        return {
            "ultimo_id_procesado": None,
            "ids_procesados": [],
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "total_procesados": 0
        }

    def save_checkpoint(self):
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(self.checkpoint, f, ensure_ascii=False, indent=2)

    def update_checkpoint(self, ultimo_id, ids_procesados):
        self.checkpoint = {
            "ultimo_id_procesado": ultimo_id,
            "ids_procesados": ids_procesados,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "total_procesados": len(ids_procesados)
        }
        self.save_checkpoint()

    def reset_checkpoint(self):
        self.checkpoint = self.default_checkpoint()
        self.save_checkpoint()
