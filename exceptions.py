# -*- coding: utf-8 -*-


class SidraError(Exception):
    """Erro-base do SIDRA Connector."""


class SidraNetworkError(SidraError):
    """Falha de rede, TLS, proxy ou conexão."""


class SidraTimeoutError(SidraNetworkError):
    """A requisição ultrapassou o timeout."""


class SidraHttpError(SidraNetworkError):
    """Resposta HTTP não bem-sucedida."""

    def __init__(self, message, status_code=None, url=None):
        super().__init__(message)
        self.status_code = status_code
        self.url = url


class SidraInvalidResponseError(SidraError):
    """O servidor respondeu, mas o conteúdo não tem o formato esperado."""


class SidraBrowserChallengeError(SidraInvalidResponseError):
    """Foi recebida uma página HTML de desafio ou bloqueio."""


class SidraQueryTooLargeError(SidraError):
    """A consulta precisa ser dividida em lotes menores."""


class SidraCancelledError(SidraError):
    """Operação cancelada pelo usuário."""


class SidraCheckpointError(SidraError):
    """Checkpoint ausente, incompatível ou corrompido."""
