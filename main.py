# VERSION V3 FINALE CORRIGÉE (consommation + vision mandat sur résidus)

from __future__ import annotations
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any
import itertools
import sys
import pandas as pd

TOLERANCE = 0.01
MAX_COMBO_SIZE = 6

# -------------------------
# NORMALISATION
# -------------------------

def norm_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip().upper()

def norm_id(value: Any) -> str:
    if value is None:
        return ""
    return "".join(c for c in str(value) if c.isalnum())

def raw_id(value: Any) -> str:
    return "" if value is None else str(value).strip()

def parse_amount(v):
    if pd.isna(v):
        return 0.0
    s = str(v).replace(",", ".").replace(" ", "")
    return round(float(s), 2)

def same_amount(a, b):
    return abs(a - b) <= TOLERANCE

def canonical_supplier(no, name):
    name = norm_text(name)
    if name == "TRESORIER TVA":
        return "TRESORIER TVA"
    no = norm_id(no)
    return no if no else name

# -------------------------
# STRUCTURES
# -------------------------

@dataclass
class MadridRow:
    account: str
    fiche: str
    mandat: str
    supplier: str
    invoice: str
    amount: float

@dataclass
class MandatRow:
    account: str
    mandat: str
    supplier: str
    invoice: str
    amount: float

# -------------------------
# CHARGEMENT
# -------------------------

def load(file):
    xl = pd.ExcelFile(file)
    return (
        pd.read_excel(file, sheet_name="MADRID"),
        pd.read_excel(file, sheet_name="Mandats")
    )

def build_madrid(df):
    rows = []
    for _, r in df.iterrows():
        rows.append(MadridRow(
            account=norm_text(r["Compte achat (fi)"]),
            fiche=raw_id(r["No Fiche (fi)"]),
            mandat=raw_id(r["No mandat (fi3)"]),
            supplier=canonical_supplier(r["Fournisseur (fi3)"], r["Raison Sociale (fi3)"]),
            invoice=norm_id(r["Réf. Facture (fi3)"]),
            amount=parse_amount(r["Actif UF (df2)"])
        ))
    return rows

def build_mandat(df):
    rows = []
    for _, r in df.iterrows():
        rows.append(MandatRow(
            account=norm_text(r["Compte Ordonnateur (cp)"]),
            mandat=raw_id(r["No Mandat (em)"]),
            supplier=canonical_supplier(r["No Fournisseur (fr)"], r["Intitulé Fournisseur (fr)"]),
            invoice=norm_id(r["Réf Fact (ml)"]),
            amount=parse_amount(r["Montant CF"])
        ))
    return rows

# -------------------------
# COMBINAISON
# -------------------------

def find_subset(values, target):
    for size in range(1, min(MAX_COMBO_SIZE, len(values)) + 1):
        for combo in itertools.combinations(values, size):
            if same_amount(sum(v for _, v in combo), target):
                return [i for i, _ in combo]
    return None

# -------------------------
# ANALYSE
# -------------------------

def analyse(madrid, mandat):

    anomalies = []
    matches = []

    used_mad = set()
    used_man = set()

    global_used_mad = set()
    global_used_man = set()

    accounts = sorted(set(x.account for x in madrid) | set(x.account for x in mandat))

    for acc in accounts:

        mads = [x for x in madrid if x.account == acc]
        mans = [x for x in mandat if x.account == acc]

        # MATCH EXACT
        for i, f in enumerate(mads):
            for j, m in enumerate(mans):
                if i in used_mad or j in used_man:
                    continue
                if f.mandat == m.mandat and same_amount(f.amount, m.amount):
                    used_mad.add(i)
                    used_man.add(j)
                    global_used_mad.add((acc, f.fiche))
                    global_used_man.add((acc, m.mandat))

        # REGROUPEMENT
        progress = True
        while progress:
            progress = False

            rem_mads = [x for i, x in enumerate(mads) if i not in used_mad]
            rem_mans = [x for j, x in enumerate(mans) if j not in used_man]

            for f in rem_mads:
                candidates = [(j, m.amount) for j, m in enumerate(rem_mans) if m.supplier == f.supplier]
                subset = find_subset(candidates, f.amount)

                if subset and len(subset) > 1:

                    global_used_mad.add((acc, f.fiche))
                    used_mad.add(mads.index(f))

                    for j in subset:
                        m = rem_mans[j]
                        global_used_man.add((acc, m.mandat))
                        used_man.add(mans.index(m))

                    progress = True
                    break

    # -------------------------
    # VISION MANDAT SUR RÉSIDUS
    # -------------------------

    for m in mandat:
        if (m.account, m.mandat) in global_used_man:
            continue

        anomalies.append({
            "vision": "Mandat",
            "compte": m.account,
            "type": "Mandat sans fiche",
            "impact": -m.amount,
            "cle": m.mandat
        })

    return anomalies

# -------------------------
# MAIN
# -------------------------

def main():
    file = sys.argv[1]
    mad_df, man_df = load(file)

    madrid = build_madrid(mad_df)
    mandat = build_mandat(man_df)

    anomalies = analyse(madrid, mandat)

    out = Path(file).with_name("resultat.xlsx")
    pd.DataFrame(anomalies).to_excel(out, index=False)

    print("OK ->", out)

if __name__ == "__main__":
    main()
