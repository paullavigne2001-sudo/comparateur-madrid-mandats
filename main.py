from __future__ import annotations

from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any
import itertools
import sys

import pandas as pd


TOLERANCE = 0.01
MAX_COMBO_SIZE = 6


MADRID_COLS = {
    "account": "Compte achat (fi)",
    "fiche": "No Fiche (fi)",
    "operation": "N° Opération fiche (fi)",
    "old_interne": "No Interne (fi)",
    "mandat": "No mandat (fi3)",
    "supplier_no": "Fournisseur (fi3)",
    "supplier_name": "Raison Sociale (fi3)",
    "invoice": "Réf. Facture (fi3)",
    "amount": "Actif UF (df2)",
}

MANDAT_COLS = {
    "account": "Compte Ordonnateur (cp)",
    "operation": "No Opération (op)",
    "mandat": "No Mandat (em)",
    "cancelled_no": "No Mdt Annul. (em)",
    "supplier_no": "No Fournisseur (fr)",
    "supplier_name": "Intitulé Fournisseur (fr)",
    "invoice": "Réf Fact (ml)",
    "order_no": "No Cde (ml)",
    "amount": "Montant CF",
}


def norm_text(value: Any) -> str:
    if value is None:
        return ""
    s = str(value).strip().upper()
    replacements = {
        "É": "E", "È": "E", "Ê": "E", "Ë": "E",
        "À": "A", "Â": "A", "Ä": "A",
        "Î": "I", "Ï": "I",
        "Ô": "O", "Ö": "O",
        "Ù": "U", "Û": "U", "Ü": "U",
        "Ç": "C",
    }
    for src, dst in replacements.items():
        s = s.replace(src, dst)
    return " ".join(s.split())


def norm_id(value: Any) -> str:
    s = norm_text(value)
    if s.endswith(".0"):
        s = s[:-2]
    return "".join(ch for ch in s if ch.isalnum())


def raw_id(value: Any) -> str:
    if value is None:
        return ""
    s = str(value).strip()
    if s.endswith(".0"):
        s = s[:-2]
    return s


def parse_amount(value: Any) -> float:
    if value is None or value == "":
        return 0.0
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return round(float(value), 2)

    s = str(value).strip().replace("\xa0", " ").replace(" ", "")
    if "," in s and "." in s:
        if s.rfind(",") > s.rfind("."):
            s = s.replace(".", "").replace(",", ".")
        else:
            s = s.replace(",", "")
    elif "," in s:
        s = s.replace(",", ".")
    try:
        return round(float(s), 2)
    except ValueError:
        return 0.0


def same_amount(a: float, b: float, tol: float = TOLERANCE) -> bool:
    return abs(a - b) <= tol


def safe_int(value: Any) -> int | None:
    s = raw_id(value)
    digits = "".join(ch for ch in s if ch.isdigit())
    return int(digits) if digits else None


def canonical_supplier(no: Any, name: Any) -> str:
    supplier_name = norm_text(name)

    if supplier_name == "TRESORIER TVA":
        return "TRESORIER TVA"

    supplier_no = norm_id(no)
    if supplier_no:
        return supplier_no

    return supplier_name


@dataclass
class MadridRow:
    account: str
    fiche_ref: str
    operation: str
    operation_raw: str
    mandat: str
    mandat_raw: str
    supplier_key: str
    supplier_name: str
    invoice: str
    amount: float


@dataclass
class MandatRow:
    account: str
    operation: str
    operation_raw: str
    mandat: str
    mandat_raw: str
    mandat_num: int | None
    cancelled_no: str
    order_no: str
    supplier_key: str
    supplier_name: str
    invoice: str
    amount: float


@dataclass
class MatchResult:
    compte: str
    etape: str
    fiche: str
    mandat_madrid: str
    mandat_mandat: str
    fournisseur: str
    montant_madrid: float
    montant_mandat: float
    statut: str
    score_confiance: int


@dataclass
class Anomaly:
    priorite: str
    vision: str
    compte: str
    type: str
    impact: float
    cle: str
    ref_fiche: str
    constat: str
    action: str
    detail: str


@dataclass
class CompteSummary:
    compte: str
    total_madrid: float
    total_mandats: float
    ecart_global: float
    nb_anomalies_metier: int
    nb_alertes_mandat: int
    controle_final: str


def load_workbook(path: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    xl = pd.ExcelFile(path)
    required = {"MADRID", "Mandats"}
    missing = required - set(xl.sheet_names)
    if missing:
        raise ValueError(f"Onglets manquants : {', '.join(sorted(missing))}")
    return (
        pd.read_excel(path, sheet_name="MADRID"),
        pd.read_excel(path, sheet_name="Mandats"),
    )


def build_madrid_rows(df: pd.DataFrame) -> list[MadridRow]:
    rows: list[MadridRow] = []
    for _, row in df.iterrows():
        op_raw = row.get(MADRID_COLS["operation"], "")
        if pd.isna(op_raw) or str(op_raw).strip() == "":
            op_raw = row.get(MADRID_COLS["old_interne"], "")

        rows.append(
            MadridRow(
                account=norm_text(row.get(MADRID_COLS["account"], "")) or "(VIDE)",
                fiche_ref=raw_id(row.get(MADRID_COLS["fiche"], "")),
                operation=norm_id(op_raw),
                operation_raw=raw_id(op_raw),
                mandat=norm_id(row.get(MADRID_COLS["mandat"], "")),
                mandat_raw=raw_id(row.get(MADRID_COLS["mandat"], "")),
                supplier_key=canonical_supplier(
                    row.get(MADRID_COLS["supplier_no"], ""),
                    row.get(MADRID_COLS["supplier_name"], ""),
                ),
                supplier_name=norm_text(row.get(MADRID_COLS["supplier_name"], "")),
                invoice=norm_id(row.get(MADRID_COLS["invoice"], "")),
                amount=parse_amount(row.get(MADRID_COLS["amount"], 0)),
            )
        )
    return rows


def build_mandat_rows(df: pd.DataFrame) -> list[MandatRow]:
    rows: list[MandatRow] = []
    for _, row in df.iterrows():
        mandat_raw = row.get(MANDAT_COLS["mandat"], "")
        rows.append(
            MandatRow(
                account=norm_text(row.get(MANDAT_COLS["account"], "")) or "(VIDE)",
                operation=norm_id(row.get(MANDAT_COLS["operation"], "")),
                operation_raw=raw_id(row.get(MANDAT_COLS["operation"], "")),
                mandat=norm_id(mandat_raw),
                mandat_raw=raw_id(mandat_raw),
                mandat_num=safe_int(mandat_raw),
                cancelled_no=raw_id(row.get(MANDAT_COLS["cancelled_no"], "")),
                order_no=raw_id(row.get(MANDAT_COLS["order_no"], "")),
                supplier_key=canonical_supplier(
                    row.get(MANDAT_COLS["supplier_no"], ""),
                    row.get(MANDAT_COLS["supplier_name"], ""),
                ),
                supplier_name=norm_text(row.get(MANDAT_COLS["supplier_name"], "")),
                invoice=norm_id(row.get(MANDAT_COLS["invoice"], "")),
                amount=parse_amount(row.get(MANDAT_COLS["amount"], 0)),
            )
        )
    return rows


def aggregate_mandats_with_annulations(rows: list[MandatRow]) -> tuple[list[MandatRow], list[dict[str, Any]]]:
    grouped: dict[tuple[str, str, str, str, str, str], list[MandatRow]] = {}
    for row in rows:
        key = (
            row.account,
            row.mandat,
            row.supplier_key,
            row.invoice,
            row.operation,
            row.order_no,
        )
        grouped.setdefault(key, []).append(row)

    active: list[MandatRow] = []
    neutralized: list[dict[str, Any]] = []

    for items in grouped.values():
        total = round(sum(x.amount for x in items), 2)
        pos = round(sum(x.amount for x in items if x.amount > 0), 2)
        neg = round(sum(x.amount for x in items if x.amount < 0), 2)
        first = items[0]

        if same_amount(total, 0.0) and len(items) > 1 and not same_amount(pos, 0.0) and not same_amount(neg, 0.0):
            neutralized.append(
                {
                    "compte": first.account,
                    "mandat": first.mandat_raw or "(vide)",
                    "fournisseur": first.supplier_name or first.supplier_key,
                    "facture": first.invoice or "-",
                    "operation": first.operation_raw or "-",
                    "commande": first.order_no or "-",
                    "total_positif": pos,
                    "total_negatif": neg,
                    "net": total,
                }
            )
            continue

        if not same_amount(total, 0.0):
            active.append(
                MandatRow(
                    account=first.account,
                    operation=first.operation,
                    operation_raw=first.operation_raw,
                    mandat=first.mandat,
                    mandat_raw=first.mandat_raw,
                    mandat_num=first.mandat_num,
                    cancelled_no=first.cancelled_no,
                    order_no=first.order_no,
                    supplier_key=first.supplier_key,
                    supplier_name=first.supplier_name,
                    invoice=first.invoice,
                    amount=total,
                )
            )

    return active, neutralized


def attach_tresorier_tva(rows: list[MandatRow]) -> tuple[list[MandatRow], list[dict[str, Any]]]:
    normal = [r for r in rows if r.supplier_key != "TRESORIER TVA"]
    tva_rows = [r for r in rows if r.supplier_key == "TRESORIER TVA"]
    links: list[dict[str, Any]] = []

    for tva in tva_rows:
        best: MandatRow | None = None
        best_score = -1

        for cand in normal:
            if cand.account != tva.account:
                continue

            score = 0
            if tva.invoice and cand.invoice and tva.invoice == cand.invoice:
                score += 100
            if tva.order_no and cand.order_no and tva.order_no == cand.order_no:
                score += 60
            if tva.mandat_num is not None and cand.mandat_num is not None and abs(tva.mandat_num - cand.mandat_num) == 1:
                score += 40
            if tva.operation and cand.operation and tva.operation == cand.operation:
                score += 20

            if score > best_score:
                best_score = score
                best = cand

        if best and best_score >= 40:
            best.amount = round(best.amount + tva.amount, 2)
            links.append(
                {
                    "compte": tva.account,
                    "mandat_tva": tva.mandat_raw,
                    "mandat_principal": best.mandat_raw,
                    "facture": tva.invoice or best.invoice,
                    "commande": tva.order_no or best.order_no,
                    "operation": tva.operation_raw or best.operation_raw,
                    "montant_tva": tva.amount,
                    "montant_principal_apres_fusion": best.amount,
                    "score": best_score,
                }
            )
        else:
            normal.append(tva)
            links.append(
                {
                    "compte": tva.account,
                    "mandat_tva": tva.mandat_raw,
                    "mandat_principal": "",
                    "facture": tva.invoice,
                    "commande": tva.order_no,
                    "operation": tva.operation_raw,
                    "montant_tva": tva.amount,
                    "montant_principal_apres_fusion": "",
                    "score": 0,
                }
            )

    return normal, links


def find_subset_sum_amounts(values: list[tuple[int, float]], target: float, max_size: int = MAX_COMBO_SIZE) -> list[int] | None:
    target = round(target, 2)
    if target <= 0:
        return None

    for size in range(1, min(max_size, len(values)) + 1):
        for combo in itertools.combinations(values, size):
            total = round(sum(v for _, v in combo), 2)
            if same_amount(total, target):
                return [idx for idx, _ in combo]
    return None


def analyse(
    madrid_rows: list[MadridRow],
    mandat_rows: list[MandatRow],
) -> tuple[
    list[Anomaly],
    list[MatchResult],
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[CompteSummary],
    list[dict[str, Any]],
]:
    anomalies: list[Anomaly] = []
    matches: list[MatchResult] = []
    reimputations: list[dict[str, Any]] = []

    mandat_net, neutralized = aggregate_mandats_with_annulations(mandat_rows)
    mandat_net, tva_links = attach_tresorier_tva(mandat_net)

    accounts = sorted({r.account for r in madrid_rows} | {r.account for r in mandat_net})

    # consommation pour filtrer la vision mandat
    consumed_madrid_for_strict: set[tuple[str, str]] = set()
    consumed_mandat_for_strict: set[tuple[str, str]] = set()

    for account in accounts:
        mads = [r for r in madrid_rows if r.account == account]
        mans = [r for r in mandat_net if r.account == account]

        used_mad: set[int] = set()
        used_man: set[int] = set()

        # 1. même mandat + même montant
        for i, f in enumerate(mads):
            for j, m in enumerate(mans):
                if i in used_mad or j in used_man:
                    continue
                if f.mandat and m.mandat and f.mandat == m.mandat and same_amount(f.amount, m.amount):
                    used_mad.add(i)
                    used_man.add(j)
                    consumed_madrid_for_strict.add((account, f.fiche_ref))
                    consumed_mandat_for_strict.add((account, m.mandat_raw))
                    matches.append(
                        MatchResult(
                            compte=account,
                            etape="1. Mandat + montant",
                            fiche=f.fiche_ref,
                            mandat_madrid=f.mandat_raw,
                            mandat_mandat=m.mandat_raw,
                            fournisseur=f.supplier_name or f.supplier_key,
                            montant_madrid=f.amount,
                            montant_mandat=m.amount,
                            statut="Rapprochement évident",
                            score_confiance=100,
                        )
                    )

        # 2. même fournisseur + même montant
        for i, f in enumerate(mads):
            if i in used_mad:
                continue
            for j, m in enumerate(mans):
                if i in used_mad or j in used_man:
                    continue
                if f.supplier_key == m.supplier_key and same_amount(f.amount, m.amount):
                    used_mad.add(i)
                    used_man.add(j)
                    consumed_madrid_for_strict.add((account, f.fiche_ref))
                    consumed_mandat_for_strict.add((account, m.mandat_raw))
                    statut = "Mandat probablement erroné sur la fiche" if f.mandat != m.mandat else "Rapprochement probable"
                    matches.append(
                        MatchResult(
                            compte=account,
                            etape="2. Fournisseur + montant",
                            fiche=f.fiche_ref,
                            mandat_madrid=f.mandat_raw,
                            mandat_mandat=m.mandat_raw,
                            fournisseur=f.supplier_name or f.supplier_key,
                            montant_madrid=f.amount,
                            montant_mandat=m.amount,
                            statut=statut,
                            score_confiance=80,
                        )
                    )
                    if f.mandat != m.mandat:
                        anomalies.append(
                            Anomaly(
                                priorite="Élevée",
                                vision="Métier",
                                compte=account,
                                type="Mandat probablement erroné",
                                impact=0.0,
                                cle=f"{f.mandat_raw or '(vide)'} -> {m.mandat_raw or '(vide)'}",
                                ref_fiche=f.fiche_ref,
                                constat="Même fournisseur et même montant, mais mandat différent.",
                                action="Vérifier si le n° mandat porté par la fiche MADRID est erroné.",
                                detail=f"Fiche {f.fiche_ref or '(vide)'} / fournisseur {f.supplier_name or f.supplier_key}",
                            )
                        )

        # 3. split 1 mandat -> plusieurs fiches, AVEC CONSOMMATION
        progress = True
        while progress:
            progress = False

            rem_mads = [r for i, r in enumerate(mads) if i not in used_mad]
            rem_mans = [r for j, r in enumerate(mans) if j not in used_man]

            for m in rem_mans:
                candidate_fiches = [
                    (idx, f.amount) for idx, f in enumerate(rem_mads)
                    if f.supplier_key == m.supplier_key
                ]
                subset = find_subset_sum_amounts(candidate_fiches, m.amount)

                if subset and len(subset) > 1:
                    fiche_refs = [rem_mads[idx].fiche_ref for idx in subset]
                    matches.append(
                        MatchResult(
                            compte=account,
                            etape="3. Split 1 mandat -> N fiches",
                            fiche=", ".join(fiche_refs),
                            mandat_madrid="",
                            mandat_mandat=m.mandat_raw,
                            fournisseur=m.supplier_name or m.supplier_key,
                            montant_madrid=round(sum(rem_mads[idx].amount for idx in subset), 2),
                            montant_mandat=m.amount,
                            statut="Split détecté",
                            score_confiance=75,
                        )
                    )

                    original_m_index = mans.index(m)
                    used_man.add(original_m_index)
                    consumed_mandat_for_strict.add((account, m.mandat_raw))

                    for idx in subset:
                        original_f_index = mads.index(rem_mads[idx])
                        used_mad.add(original_f_index)
                        consumed_madrid_for_strict.add((account, rem_mads[idx].fiche_ref))

                    progress = True
                    break

        # 4. regroupement N mandats -> 1 fiche, AVEC CONSOMMATION
        progress = True
        while progress:
            progress = False

            rem_mads = [r for i, r in enumerate(mads) if i not in used_mad]
            rem_mans = [r for j, r in enumerate(mans) if j not in used_man]

            for f in rem_mads:
                candidate_mandats = [
                    (j, m.amount) for j, m in enumerate(rem_mans)
                    if m.supplier_key == f.supplier_key
                ]
                subset = find_subset_sum_amounts(candidate_mandats, f.amount)

                if subset and len(subset) > 1:
                    mandat_refs = [rem_mans[j].mandat_raw for j in subset]

                    matches.append(
                        MatchResult(
                            compte=account,
                            etape="4. Regroupement N mandats -> 1 fiche",
                            fiche=f.fiche_ref,
                            mandat_madrid=f.mandat_raw,
                            mandat_mandat=", ".join(mandat_refs),
                            fournisseur=f.supplier_name or f.supplier_key,
                            montant_madrid=f.amount,
                            montant_mandat=round(sum(rem_mans[j].amount for j in subset), 2),
                            statut="Regroupement détecté",
                            score_confiance=75,
                        )
                    )

                    original_f_index = mads.index(f)
                    used_mad.add(original_f_index)
                    consumed_madrid_for_strict.add((account, f.fiche_ref))

                    for j in subset:
                        original_m_index = mans.index(rem_mans[j])
                        used_man.add(original_m_index)
                        consumed_mandat_for_strict.add((account, rem_mans[j].mandat_raw))

                    progress = True
                    break

        rem_mads = [r for i, r in enumerate(mads) if i not in used_mad]
        rem_mans = [r for j, r in enumerate(mans) if j not in used_man]

        # 5. analyse résiduelle
        for f in rem_mads:
            same_supplier = [m for m in rem_mans if m.supplier_key == f.supplier_key]
            total_supplier = round(sum(m.amount for m in same_supplier), 2)
            gap = round(f.amount - total_supplier, 2)

            if not same_supplier:
                cross_account = [
                    x for x in madrid_rows
                    if x.account != account and x.supplier_key == f.supplier_key and same_amount(x.amount, f.amount)
                ]
                if cross_account:
                    for other in cross_account[:5]:
                        reimputations.append(
                            {
                                "compte_source": other.account,
                                "compte_cible": account,
                                "fiche": other.fiche_ref,
                                "fournisseur": other.supplier_name or other.supplier_key,
                                "montant": other.amount,
                            }
                        )
                anomalies.append(
                    Anomaly(
                        priorite="Critique",
                        vision="Métier",
                        compte=account,
                        type="Fiche sans mandat",
                        impact=f.amount,
                        cle=f.fiche_ref or "(vide)",
                        ref_fiche=f.fiche_ref,
                        constat="Aucun mandat restant du même fournisseur ne correspond à cette fiche.",
                        action="Supprimer la fiche si elle est erronée ou rechercher un mandat sur un autre compte.",
                        detail=(
                            f"Fiche {f.fiche_ref or '(vide)'} / montant {f.amount:.2f}"
                            + (" / suspicion de réimputation inter-compte" if cross_account else "")
                        ),
                    )
                )
                continue

            if gap > 0:
                candidate_mandats = [(j, m.amount) for j, m in enumerate(same_supplier)]
                subset = find_subset_sum_amounts(candidate_mandats, gap)
                if subset:
                    subset_ids = ", ".join(same_supplier[j].mandat_raw or "(vide)" for j in subset)
                    anomalies.append(
                        Anomaly(
                            priorite="Élevée",
                            vision="Métier",
                            compte=account,
                            type="Mandats complémentaires identifiés",
                            impact=gap,
                            cle=f.fiche_ref or "(vide)",
                            ref_fiche=f.fiche_ref,
                            constat="Une combinaison de mandats semble expliquer l'écart restant.",
                            action="Contrôler ces mandats complémentaires avant correction.",
                            detail=f"Mandats candidats : {subset_ids}",
                        )
                    )
                else:
                    anomalies.append(
                        Anomaly(
                            priorite="Critique",
                            vision="Métier",
                            compte=account,
                            type="Manque mandat",
                            impact=gap,
                            cle=f.fiche_ref or "(vide)",
                            ref_fiche=f.fiche_ref,
                            constat="Le montant de la fiche dépasse les mandats restants disponibles.",
                            action="Chercher un mandat complémentaire, un mandat TRESORIER TVA ou un oubli de mandat.",
                            detail=f"Fiche {f.amount:.2f} / mandats fournisseur {total_supplier:.2f} / écart {gap:.2f}",
                        )
                    )
            elif gap < 0:
                candidate_mandats = [(j, m.amount) for j, m in enumerate(same_supplier)]
                subset = find_subset_sum_amounts(candidate_mandats, abs(gap))
                if subset:
                    subset_ids = ", ".join(same_supplier[j].mandat_raw or "(vide)" for j in subset)
                    anomalies.append(
                        Anomaly(
                            priorite="Critique",
                            vision="Métier",
                            compte=account,
                            type="Mandat sans fiche",
                            impact=gap,
                            cle=f.fiche_ref or "(vide)",
                            ref_fiche=f.fiche_ref,
                            constat="Une combinaison de mandats semble excéder le montant de la fiche.",
                            action="Créer une fiche complémentaire ou vérifier un mandat sans fiche.",
                            detail=f"Mandats excédentaires probables : {subset_ids}",
                        )
                    )
                else:
                    anomalies.append(
                        Anomaly(
                            priorite="Critique",
                            vision="Métier",
                            compte=account,
                            type="Fiche insuffisante",
                            impact=gap,
                            cle=f.fiche_ref or "(vide)",
                            ref_fiche=f.fiche_ref,
                            constat="Les mandats restants du fournisseur dépassent le montant de la fiche.",
                            action="Compléter la fiche, créer une fiche supplémentaire ou vérifier le montant saisi.",
                            detail=f"Fiche {f.amount:.2f} / mandats fournisseur {total_supplier:.2f} / écart {gap:.2f}",
                        )
                    )

        for m in rem_mans:
            cross_account = any(
                f.account != account and f.supplier_key == m.supplier_key and same_amount(f.amount, m.amount)
                for f in madrid_rows
            )
            anomalies.append(
                Anomaly(
                    priorite="Critique",
                    vision="Métier",
                    compte=account,
                    type="Mandat sans fiche",
                    impact=-m.amount,
                    cle=m.mandat_raw or "(vide)",
                    ref_fiche="",
                    constat="Ce mandat reste sans fiche MADRID sur ce compte.",
                    action="Créer la fiche manquante ou vérifier une réimputation de compte." if cross_account else "Créer la fiche manquante.",
                    detail=f"Mandat {m.mandat_raw or '(vide)'} / montant {m.amount:.2f}",
                )
            )

    # lecture stricte secondaire
    strict_madrid: dict[tuple[str, str, str, str, str], float] = {}
    strict_fiche_refs: dict[tuple[str, str, str, str, str], str] = {}
    for r in madrid_rows:
        key = (r.account, r.mandat, r.supplier_key, r.invoice, r.operation)
        strict_madrid[key] = round(strict_madrid.get(key, 0.0) + r.amount, 2)
        existing = strict_fiche_refs.get(key, "")
        strict_fiche_refs[key] = ", ".join(x for x in [existing, r.fiche_ref] if x).strip(", ")

    strict_mandat: dict[tuple[str, str, str, str, str], float] = {}
    strict_mandat_raw: dict[tuple[str, str, str, str, str], str] = {}
    for r in mandat_net:
        key = (r.account, r.mandat, r.supplier_key, r.invoice, r.operation)
        strict_mandat[key] = round(strict_mandat.get(key, 0.0) + r.amount, 2)
        strict_mandat_raw[key] = r.mandat_raw

    all_keys = sorted(set(strict_madrid) | set(strict_mandat))
    strict_rows: list[dict[str, Any]] = []
    for key in all_keys:
        mad_amt = strict_madrid.get(key, 0.0)
        man_amt = strict_mandat.get(key, 0.0)
        gap = round(mad_amt - man_amt, 2)
        account, mandat, supplier, invoice, operation = key
        strict_mandat_ref = strict_mandat_raw.get(key, mandat or "(vide)")
        strict_fiche_ref = strict_fiche_refs.get(key, "")

        strict_rows.append(
            {
                "compte": account,
                "mandat": strict_mandat_ref,
                "fournisseur": supplier,
                "facture": invoice or "-",
                "operation": operation or "-",
                "madrid": mad_amt,
                "mandats": man_amt,
                "ecart": gap,
            }
        )

        is_consumed_by_metier = (
            (account, strict_mandat_ref) in consumed_mandat_for_strict
            or any((account, f.strip()) in consumed_madrid_for_strict for f in strict_fiche_ref.split(",") if f.strip())
        )

        if not same_amount(gap, 0.0) and not is_consumed_by_metier:
            anomalies.append(
                Anomaly(
                    priorite="Info",
                    vision="Mandat",
                    compte=account,
                    type="Écart technique mandat",
                    impact=gap,
                    cle=strict_mandat_ref,
                    ref_fiche=strict_fiche_ref,
                    constat="Écart sur la clé stricte mandat/fournisseur/facture/opération.",
                    action="Contrôler le rattachement strict du mandat.",
                    detail=f"MADRID {mad_amt:.2f} - MANDATS {man_amt:.2f} = {gap:.2f}",
                )
            )

    summaries: list[CompteSummary] = []
    for account in accounts:
        total_madrid = round(sum(x.amount for x in madrid_rows if x.account == account), 2)
        total_mandats = round(sum(x.amount for x in mandat_rows if x.account == account), 2)
        ecart_global = round(total_madrid - total_mandats, 2)
        nb_metier = sum(1 for x in anomalies if x.compte == account and x.vision == "Métier")
        nb_mandat = sum(1 for x in anomalies if x.compte == account and x.vision == "Mandat")
        summaries.append(
            CompteSummary(
                compte=account,
                total_madrid=total_madrid,
                total_mandats=total_mandats,
                ecart_global=ecart_global,
                nb_anomalies_metier=nb_metier,
                nb_alertes_mandat=nb_mandat,
                controle_final="OK" if nb_metier == 0 else "ANOMALIES",
            )
        )

    return anomalies, matches, strict_rows, neutralized, tva_links, summaries, reimputations


def export_results(
    output_path: Path,
    anomalies: list[Anomaly],
    matches: list[MatchResult],
    strict_rows: list[dict[str, Any]],
    neutralized: list[dict[str, Any]],
    tva_links: list[dict[str, Any]],
    summaries: list[CompteSummary],
    reimputations: list[dict[str, Any]],
) -> None:
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        pd.DataFrame([asdict(x) for x in anomalies]).to_excel(writer, sheet_name="anomalies", index=False)
        pd.DataFrame([asdict(x) for x in matches]).to_excel(writer, sheet_name="matches", index=False)
        pd.DataFrame(strict_rows).to_excel(writer, sheet_name="controle_mandat", index=False)
        pd.DataFrame([asdict(x) for x in summaries]).to_excel(writer, sheet_name="synthese_comptes", index=False)
        pd.DataFrame(neutralized).to_excel(writer, sheet_name="mandats_neutralises", index=False)
        pd.DataFrame(tva_links).to_excel(writer, sheet_name="tva_links", index=False)
        pd.DataFrame(reimputations).to_excel(writer, sheet_name="reimputations_suspectes", index=False)


def main() -> None:
    if len(sys.argv) < 2:
        print("Usage : python main.py <fichier_excel.xlsx>")
        sys.exit(1)

    input_path = Path(sys.argv[1])
    if not input_path.exists():
        print(f"Fichier introuvable : {input_path}")
        sys.exit(1)

    madrid_df, mandat_df = load_workbook(input_path)
    madrid_rows = build_madrid_rows(madrid_df)
    mandat_rows = build_mandat_rows(mandat_df)

    anomalies, matches, strict_rows, neutralized, tva_links, summaries, reimputations = analyse(
        madrid_rows, mandat_rows
    )

    output_path = input_path.with_name(input_path.stem + "_resultat_v3.xlsx")
    export_results(
        output_path,
        anomalies,
        matches,
        strict_rows,
        neutralized,
        tva_links,
        summaries,
        reimputations,
    )

    print("Analyse terminée.")
    print(f"- anomalies : {len(anomalies)}")
    print(f"- matches : {len(matches)}")
    print(f"- mandats neutralisés : {len(neutralized)}")
    print(f"- rattachements TVA : {len(tva_links)}")
    print(f"- réimputations suspectes : {len(reimputations)}")
    print(f"Fichier exporté : {output_path}")


if __name__ == "__main__":
    main()
