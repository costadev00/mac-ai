from typing import List, Dict
from babel.numbers import format_currency
import inflect

_inflect = inflect.engine()


def _plural(word: str, qty: int) -> str:
    """Return word in singular or plural based on qty."""
    return word if qty == 1 else (_inflect.plural_noun(word) or f"{word}s")


def make_human_answer(pergunta_pt: str, rows: List[Dict], sql: str) -> str:
    """Converte linhas SQL em um parágrafo amigável em português."""
    prefixo = "A Distribuidora XYZ"
    if not rows:
        return f"{prefixo} não encontrou resultados para \"{pergunta_pt}\"."

    n = len(rows)
    resultado_palavra = _plural("resultado", n)
    partes = [f"{prefixo} encontrou {n} {resultado_palavra} para \"{pergunta_pt}\"."]

    # Detect field com nome do item
    nome_campo = None
    for campo in ("produto", "nome", "item"):
        if campo in rows[0]:
            nome_campo = campo
            break

    if nome_campo:
        contagem = {}
        for r in rows:
            nome = r.get(nome_campo)
            if nome is None:
                continue
            contagem[nome] = contagem.get(nome, 0) + 1
        if contagem:
            itens = [f"{qtd} {_plural(nome, qtd)}" for nome, qtd in contagem.items()]
            partes.append("; ".join(itens) + ".")

    # Somar colunas numéricas
    totais = {}
    for r in rows:
        for chave, valor in r.items():
            if isinstance(valor, (int, float)):
                totais.setdefault(chave, 0)
                totais[chave] += valor
    if totais:
        partes_somas = []
        for chave, total in totais.items():
            if any(k in chave.lower() for k in ["valor", "preco", "preço", "total"]):
                val_str = format_currency(total, "BRL", locale="pt_BR")
            else:
                val_str = str(total)
            partes_somas.append(f"{chave} {val_str}")
        partes.append("Somatórios: " + ", ".join(partes_somas) + ".")

    partes.append(f"SQL utilizado: {sql}")
    return " ".join(partes)
