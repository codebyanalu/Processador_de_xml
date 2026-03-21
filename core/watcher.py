"""
core/watcher.py
Monitora arquivos .py do projeto e recarrega módulos automaticamente ao detectar mudanças.
Funciona em background — não bloqueia a UI.

Módulos recarregáveis:
  - config.settings
  - extract.xml_reader
  - extract.nfse_reader
  - transform.validator
  - load.storage

Limitação intencional: main_window.py NÃO é recarregado (impossível recarregar
a janela tkinter enquanto está rodando). Para mudanças na UI, reinicie o sistema.
"""

import importlib
import os
import sys
import threading
import time
from datetime import datetime

# Módulos monitorados — na ordem correta de dependência
MODULOS_MONITORADOS = [
    "config.settings",
    "extract.xml_reader",
    "extract.nfse_reader",
    "transform.validator",
    "load.storage",
    "load",
    "extract",
    "transform",
]

# Intervalo de verificação em segundos
INTERVALO = 1.5


def _arquivo_do_modulo(nome_modulo: str) -> str | None:
    """Retorna o caminho do arquivo .py de um módulo já importado."""
    mod = sys.modules.get(nome_modulo)
    if mod is None:
        return None
    spec = getattr(mod, "__spec__", None)
    if spec and spec.origin and spec.origin.endswith(".py"):
        return spec.origin
    arquivo = getattr(mod, "__file__", None)
    if arquivo and arquivo.endswith(".py"):
        return arquivo
    return None


def _mtime(caminho: str) -> float:
    """Retorna o timestamp de modificação do arquivo."""
    try:
        return os.path.getmtime(caminho)
    except Exception:
        return 0.0


class FileWatcher:
    """
    Watcher que roda em thread daemon e recarrega módulos quando seus
    arquivos .py são modificados.

    Uso:
        watcher = FileWatcher(callback=minha_funcao_de_log)
        watcher.start()
        # ... mais tarde, ao fechar:
        watcher.stop()
    """

    def __init__(self, callback=None):
        """
        callback: função chamada com (nome_modulo, caminho) quando um módulo
                  é recarregado. Útil para logar na UI.
        """
        self._callback = callback or (lambda mod, arq: None)
        self._rodando  = False
        self._thread   = None
        self._mtimes   = {}  # {nome_modulo: mtime}

    def start(self):
        """Inicia o watcher em thread daemon."""
        if self._rodando:
            return
        self._rodando = True
        self._thread  = threading.Thread(target=self._loop, daemon=True, name="FileWatcher")
        self._thread.start()

    def stop(self):
        """Para o watcher."""
        self._rodando = False

    def _loop(self):
        # Captura os mtimes iniciais (snapshot no momento do start)
        for nome in MODULOS_MONITORADOS:
            arq = _arquivo_do_modulo(nome)
            if arq:
                self._mtimes[nome] = _mtime(arq)

        while self._rodando:
            time.sleep(INTERVALO)
            for nome in MODULOS_MONITORADOS:
                arq = _arquivo_do_modulo(nome)
                if not arq:
                    continue
                mtime_atual = _mtime(arq)
                mtime_salvo = self._mtimes.get(nome, 0.0)
                if mtime_atual > mtime_salvo:
                    self._mtimes[nome] = mtime_atual
                    self._recarregar(nome, arq)

    def _recarregar(self, nome: str, arq: str):
        """Recarrega o módulo e chama o callback."""
        try:
            mod = sys.modules.get(nome)
            if mod is not None:
                importlib.reload(mod)
            hora = datetime.now().strftime("%H:%M:%S")
            self._callback(nome, arq, hora, sucesso=True, erro=None)
        except Exception as e:
            hora = datetime.now().strftime("%H:%M:%S")
            self._callback(nome, arq, hora, sucesso=False, erro=str(e))
