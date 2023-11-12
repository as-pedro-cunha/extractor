from marvin import ai_classifier
from enum import Enum
from extractor.tutorials.files import get_nf1_xml


@ai_classifier
class NfKind(Enum):
    """Representa a categoria de Notas Fiscais dado os itens de uma nota fiscal."""

    TI = "TI"
    MARKETING = "Marketing"
    FINANCEIRO = "Financeiro"
    JURIDICO = "Jurídico"
    RH = "RH"
    COMERCIAL = "Comercial"
    LOGISTICA = "Logística"
    GADO = "Gado"


nf_kind = NfKind(get_nf1_xml())
print(nf_kind.value)
