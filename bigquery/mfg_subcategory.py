"""
Manufacturing spend sub-category classifier.

Assigns each line item a mfg_subcategory based on multi-signal scoring:
  1. Vendor identity  (strongest signal for specialist vendors)
  2. Description keywords  (strongest for multi-category vendors & Odoo lines)
  3. Project / Ramp-card context  (disambiguates generic purchases)
  4. Price heuristics  (separates capital equipment from parts)

Sub-categories are designed for a manufacturing accountant:
  - 10 manufacturing-relevant buckets
  - 3 non-manufacturing buckets for clean separation
"""
from __future__ import annotations

import re
from enum import Enum
from typing import Any

import pandas as pd


# ---------------------------------------------------------------------------
# Sub-category definitions
# ---------------------------------------------------------------------------

class SubCat(str, Enum):
    PROCESS_EQUIPMENT = "Process Equipment"
    CONTROLS_ELECTRICAL = "Controls & Electrical"
    MECHANICAL_STRUCTURAL = "Mechanical & Structural"
    CONSUMABLES = "Consumables"
    MFG_TOOLS_SUPPLIES = "MFG Tools & Shop Supplies"
    DESIGN_ENGINEERING = "Design & Engineering Services"
    INTEGRATION_COMMISSIONING = "Integration & Commissioning"
    QUALITY_METROLOGY = "Quality & Metrology"
    SOFTWARE_LICENSES = "Software & Licenses"
    SHIPPING_FREIGHT = "Shipping & Freight"
    FACILITIES_OFFICE = "Facilities & Office"
    IT_EQUIPMENT = "IT Equipment"
    GENERAL_ADMIN = "General & Administrative"


MFG_SUBCATS: set[str] = {
    SubCat.PROCESS_EQUIPMENT.value,
    SubCat.CONTROLS_ELECTRICAL.value,
    SubCat.MECHANICAL_STRUCTURAL.value,
    SubCat.CONSUMABLES.value,
    SubCat.MFG_TOOLS_SUPPLIES.value,
    SubCat.DESIGN_ENGINEERING.value,
    SubCat.INTEGRATION_COMMISSIONING.value,
    SubCat.QUALITY_METROLOGY.value,
    SubCat.SOFTWARE_LICENSES.value,
    SubCat.SHIPPING_FREIGHT.value,
}


# ---------------------------------------------------------------------------
# Signal 1 — Specialist vendor mapping
# ---------------------------------------------------------------------------

_VENDOR_MAP: dict[str, str] = {}


def _v(names: list[str], subcat: SubCat) -> None:
    for n in names:
        _VENDOR_MAP[n.lower()] = subcat.value


_v([
    "mission design & automation", "fanuc america", "bond technologies",
    "design & assembly concepts", "gp reeves", "leakmaster", "trumpf",
    "ellsworth dispensing solutions", "ergomat", "ametek", "rexgear",
    "grizzly industrial", "foxalien", "serra laser & waterjet",
    "evolution motion solutions",
], SubCat.PROCESS_EQUIPMENT)

_v([
    "awc, inc", "balluff", "ifm efector", "cognex", "digi-key", "digikey",
    "mouser electronics", "automationdirect", "galco industrial",
    "elliott electric", "gordon electric", "elecdirect", "verivolt",
    "anchor electronics", "rs group", "smc pneumatics", "hioki",
    "four-o fluid power", "power motion & industrial", "radwell international",
], SubCat.CONTROLS_ELECTRICAL)

_v([
    "pacific integrated handling", "xometry", "american precision engineering",
    "brandt precision machining", "babeco fabrication", "austex machine",
    "sendcutsend", "misumi", "on1design", "motor city tool & die",
    "80/20", "2f prototypes", "fictiv", "jw winco", "norelem",
    "judge tool & gage",
], SubCat.MECHANICAL_STRUCTURAL)

_v([
    "ellsworth adhesives", "gluegun.com", "sigma-aldrich", "chemical concepts",
    "lab alley", "diversified enterprises", "infinity bond", "polymaker",
    "alkali scientific", "cole-parmer", "thermo fisher",
], SubCat.CONSUMABLES)

_v([
    "kruss scientific", "defelsko", "faro technologies",
    "micro precision calibration", "checkline", "mark-10", "tekscan",
    "scalesgalore", "edmund optics", "chuck reagan", "yokogawa",
    "verder scientific",
], SubCat.QUALITY_METROLOGY)

_v(["chet colopy"], SubCat.DESIGN_ENGINEERING)

_v(["patti engineering", "cse electric"], SubCat.INTEGRATION_COMMISSIONING)

_v([
    "inductive automation", "goengineer", "flexsim", "shi international",
    "bluebeam", "softwarehubs", "trimble", "cursor", "adobe",
    "cleverbridge", "apple services", "total wireless",
    "portable technology solutions",
], SubCat.SOFTWARE_LICENSES)

_v(["dhl express", "fedex", "craters & fraiters"], SubCat.SHIPPING_FREIGHT)

_v([
    "newark electronics", "wire & cable your way", "vision automation",
    "maddox industrial", "compressor world", "fs.com", "gryphon",
], SubCat.CONTROLS_ELECTRICAL)

_v(["proto labs", "protolabs", "fixtur fab", "haas automation"],
   SubCat.MECHANICAL_STRUCTURAL)

_v(["thorlabs", "test equipment depot"], SubCat.QUALITY_METROLOGY)

_v(["hotmelt.com", "pace technologies", "supplyhous"],
   SubCat.CONSUMABLES)

_v(["brady worldwide", "cp lab safety", "motion industries",
    "otc industrial", "suppliesshops", "everlast power",
    "akon"], SubCat.MFG_TOOLS_SUPPLIES)

_v([
    "dockzilla", "art office signs", "ogd overhead garage door",
    "mirage window film", "webstaurantstore", "sharp brothers locksmith",
    "academy sports", "letsgo network", "chili's",
], SubCat.FACILITIES_OFFICE)

_v(["dell", "best buy", "b&h photo"], SubCat.IT_EQUIPMENT)


def _vendor_classify(vendor_name: str) -> str | None:
    vn = vendor_name.lower().strip()
    for fragment, subcat in _VENDOR_MAP.items():
        if fragment in vn:
            return subcat
    return None


# ---------------------------------------------------------------------------
# Signal 2 — Description keyword patterns (ordered by specificity)
# ---------------------------------------------------------------------------

_KW_RULES: list[tuple[str, re.Pattern[str]]] = [
    (SubCat.SHIPPING_FREIGHT.value, re.compile(
        r"(^shipping\b|^freight\b|^crating\b|^handling\b|^DDP\b|"
        r"ship\s*&\s*handl|shipping\s*(cost|charge|fee)|"
        r"postage|logistics|^delivery\b|trucking)", re.I)),

    (SubCat.GENERAL_ADMIN.value, re.compile(
        r"(^tariff\b|surcharge|(?<!heavy\s)(?<!medium\s)(?<!light\s)duty\b|"
        r"duties\b|customs\b)", re.I)),

    (SubCat.INTEGRATION_COMMISSIONING.value, re.compile(
        r"(controls?\s+integration|commissioning|on-?site\s+(install|support|fee|tech)"
        r"|start-?up\s+(support|service)|electrical\s+(install|scope)"
        r"|^installation\b|mechanical.*installation"
        r"|wiring\s+(scope|labor|service)|^FAT\b.*\bSAT\b"
        r"|onsite\s+tech\s+support)", re.I)),

    (SubCat.DESIGN_ENGINEERING.value, re.compile(
        r"(concepting|process\s+engineering|project\s+management"
        r"|^engineering\s+(hour|service|fee|support)"
        r"|^design\s+(service|hour|fee|review)"
        r"|^consulting\b|mechanical\s+design"
        r"|CAD\s+(design|model)|station\s+design)", re.I)),

    (SubCat.SOFTWARE_LICENSES.value, re.compile(
        r"(^software\b|fleet\s+management\s+software|license\s+key"
        r"|subscription\b|^SaaS\b|software\s+package"
        r"|programming\s+software|\.0\s+software)", re.I)),

    (SubCat.QUALITY_METROLOGY.value, re.compile(
        r"(microscope|VHX-|surface\s+analyzer|coating\s+thickness"
        r"|metrology|CMM\b|calibration\s+(service|fee)"
        r"|inspection\s+(system|equip)|gauge\b.*calibrat"
        r"|force\s+gauge|torque\s+(tester|gauge)|digital\s+power\s+meter"
        r"|measurement\s+(head|system)|3D\s+scan)", re.I)),

    (SubCat.CONSUMABLES.value, re.compile(
        r"(adhesive|^resin\b|^hardener\b|CoolTherm|thermal\s+interface"
        r"|dispense\s+tip|weld\s+wire|shielding\s+gas|solder\s+paste"
        r"|^flux\b|isopropyl|cleaning\s+solution|wipe\s*s?\b"
        r"|epoxy\b|primer\b|sealant\b|silicone\s+grease"
        r"|3D\s+print.*filament|PLA\b.*filament|ABS\b.*filament"
        r"|nozzle\s+tip|dispens.*needle)", re.I)),

    (SubCat.PROCESS_EQUIPMENT.value, re.compile(
        r"(robot\b|LR\s*Mate|M-\d+iC|R-\d+iA"
        r"|leak\s*test(er|ing)?\b|Guardian\b"
        r"|dispense\s+system|dispens(er|ing)\s+(station|machine|unit)"
        r"|weld\s+(station|system|cell|machine)|ultrasonic\s+weld"
        r"|friction\s+stir|FSW\b|TruFiber"
        r"|laser\s+(weld|source|system)|LWM\s*4\.0"
        r"|press\s+(station|machine|system)|PCBA\s+press"
        r"|functional\s+test\s+(station|system|stand)"
        r"|hipot\s+(test|station|system|tester)"
        r"|automation\s+cell|assembly\s+(cell|station|system)"
        r"|test\s+(stand|station|system|bench|fixture)\b"
        r"|bi-?directional\b.*power|power\s+supply\s+unit\s+\d+[kK]?[wW]"
        r"|torque\s+(tool|driver|wrench)\b.*system"
        r"|ILG\s+arm|torq\s*lift|lifting\s*(device|system)"
        r"|work\s*station\s+crane|jib\s+crane)", re.I)),

    (SubCat.CONTROLS_ELECTRICAL.value, re.compile(
        r"(sensor\b|photoelectric|inductive\s+sensor|proximity\s+sensor"
        r"|RFID\b|barcode\s*(reader|scanner)|^SR-X|^SR-\d"
        r"|PLC\b|HMI\b|servo\b|VFD\b|drive\b.*motor|motor\s+drive"
        r"|encoder\b|relay\b|contactor\b|circuit\s+breaker"
        r"|terminal\s+block|DIN\s+rail|power\s+supply\s+\d+[VvWw]"
        r"|connector\b.*M\d+|cable\b.*M\d+|^EVC\d|^BCC\w|^BNI\w|^BAE\w"
        r"|^PX\s*3\d|^PX\s*\d{5}"
        r"|e-?stop|emergency\s+stop|safety\s+(relay|PLC|controller|light|curtain)"
        r"|light\s+curtain|touch\s+sensor.*button|reset\s+button"
        r"|ethernet\s+switch|network\s+switch|profinet"
        r"|I/?O\s+(module|block|card)|signal\s+tower|stack\s+light"
        r"|vision\s+(camera|system|sensor)|In-?Sight\b"
        r"|industrial\s+PC|panel\s+PC|industrial\s+computer"
        r"|Multitorch|LED\s+light\b.*machine"
        r"|patch\s+cable|dc\s+patch|micro\s+dc"
        r"|profile\s+sensor|distance\s+sensor|optical.*sensor"
        r"|mounting\s+plate.*sensor|mounting\s+bracket.*sensor"
        r"|pneumatic\b|solenoid\s+valve|air\s+cylinder"
        r"|vacuum\s+(generator|pump|switch|sensor|ejector)"
        r"|pressure\s+(sensor|transducer|switch|regul)"
        r"|flow\s+(sensor|switch|meter))", re.I)),

    (SubCat.MECHANICAL_STRUCTURAL.value, re.compile(
        r"(extrusion|aluminum\s+profile|t-?nut|T\s+Nut|gusset"
        r"|^3842\d|framing\b|strut\b.*channel"
        r"|bracket\b|mounting\s+(plate|bracket|hardware)"
        r"|support\s+block|datum\s+block|clamp\b.*bar"
        r"|guarding|guard\s+panel|lexan|polycarbonate\s+panel"
        r"|sheet\s+metal|laser\s+cut\b|waterjet\s+cut"
        r"|CNC\s+machin|custom\s+machin|precision\s+machin"
        r"|weldment|fabricat|powder\s*coat"
        r"|linear\s+(rail|guide|actuator|slide|motion)"
        r"|ball\s+screw|lead\s+screw|bearing\b|bushing\b"
        r"|locating\s+pin|dowel\s+pin|shoulder\s+(bolt|screw)"
        r"|leveling\s+f(oo|ee)t|caster\b|wheel\b.*cart"
        r"|hinge\b|latch\b|handle\b.*door|door\s+panel"
        r"|pallet\b.*datum|pallet\b.*block|cart\s+assy"
        r"|lifter\b|module\s+lifter|conveyor\s+section"
        r"|foundation\s+bracket|angle\s+gusset|clean\s*room\s+hinge"
        r"|^prd-BPC-)", re.I)),

    (SubCat.MFG_TOOLS_SUPPLIES.value, re.compile(
        r"(hand\s+tool|power\s+tool|drill\b|saw\b|wrench\b"
        r"|screwdriver|plier|socket\s+set|tool\s+kit"
        r"|PPE\b|safety\s+glass|hard\s+hat|glove"
        r"|tape\b|marker\b|bin\b|storage\s+bin"
        r"|shop\s+vac|vacuum\s+cleaner|broom\b|mop\b"
        r"|workbench|shop\s+table|peg\s*board"
        r"|zip\s+tie|cable\s+tie|velcro"
        r"|anti-?fatigue\s+mat|floor\s+mat"
        r"|trash\s+can|waste\s+bin|first\s+aid"
        r"|fire\s+extinguisher|EH&?S\b"
        r"|janitorial|cleaning\s+suppli"
        r"|label\s+maker|label\s+print"
        r"|boots\b|steel\s+toe)", re.I)),

    (SubCat.FACILITIES_OFFICE.value, re.compile(
        r"(^office\b|desk\b|chair\b|monitor\s+arm|keyboard\b"
        r"|standing\s+desk|whiteboard|conference\b"
        r"|break\s*room|kitchen\b|refrigerator|microwave"
        r"|window\s+film|garage\s+door|loading\s+dock"
        r"|dock\s+leveler|signage\b|^sign\b"
        r"|parking\b|landscap|HVAC\b|air\s+condition)", re.I)),

    (SubCat.IT_EQUIPMENT.value, re.compile(
        r"(^laptop\b|^desktop\b|^monitor\b(?!.*sensor)"
        r"|printer\b(?!.*3D)|^tablet\b"
        r"|network\s+cable|^router\b|^switch\b.*port"
        r"|^server\b|^UPS\b.*battery|^NAS\b)", re.I)),
]


def _keyword_classify(description: str) -> list[tuple[str, str]]:
    hits: list[tuple[str, str]] = []
    if not description or pd.isna(description):
        return hits
    for subcat, pattern in _KW_RULES:
        m = pattern.search(description)
        if m:
            hits.append((subcat, m.group(0)))
    return hits


# ---------------------------------------------------------------------------
# Signal 3 — Project and Ramp-card context
# ---------------------------------------------------------------------------

def _project_hint(project_name: str) -> str | None:
    pn = project_name.lower().strip()
    if not pn:
        return None
    if "quality" in pn:
        return SubCat.QUALITY_METROLOGY.value
    if "facilities and infrastructure" in pn:
        return SubCat.FACILITIES_OFFICE.value
    if "manufacturing it systems" in pn:
        return SubCat.SOFTWARE_LICENSES.value
    if "warehousing and material handling" in pn:
        return SubCat.MECHANICAL_STRUCTURAL.value
    if "maintenance and spares" in pn:
        return SubCat.MFG_TOOLS_SUPPLIES.value
    return None


_RAMP_CARD_HINTS: dict[str, str] = {}


def _rc(fragments: list[str], subcat: SubCat) -> None:
    for f in fragments:
        _RAMP_CARD_HINTS[f.lower()] = subcat.value


_rc(["quality expenses", "iqc", "ipqc", "metrology"], SubCat.QUALITY_METROLOGY)
_rc(["tools and supplies for maintenance", "maintenance department"],
    SubCat.MFG_TOOLS_SUPPLIES)
_rc(["eh&s", "ehs related"], SubCat.MFG_TOOLS_SUPPLIES)
_rc(["software purchasing"], SubCat.SOFTWARE_LICENSES)
_rc(["hardware and software for industrial engineering"],
    SubCat.CONTROLS_ELECTRICAL)
_rc(["inverter eol bring up"], SubCat.CONTROLS_ELECTRICAL)
_rc(["production line parts", "agv posts"], SubCat.MECHANICAL_STRUCTURAL)
_rc(["c sample support", "b2 weldmask"], SubCat.MECHANICAL_STRUCTURAL)
_rc(["builds materials for lab work", "b2 test equipment"],
    SubCat.MFG_TOOLS_SUPPLIES)
_rc(["general expenses", "factory 1 general needs",
     "factory manufacturing supplies", "manufacturing supplies",
     "base factory 2 expenses"],
    SubCat.MFG_TOOLS_SUPPLIES)
_rc(["boots"], SubCat.MFG_TOOLS_SUPPLIES)
_rc(["travel", "international travel"], SubCat.GENERAL_ADMIN)


def _ramp_card_hint(card_name: str) -> str | None:
    cn = card_name.lower().strip()
    if not cn:
        return None
    for fragment, subcat in _RAMP_CARD_HINTS.items():
        if fragment in cn:
            return subcat
    return None


# ---------------------------------------------------------------------------
# Signal 4 — General distributor handling
# ---------------------------------------------------------------------------

_GENERAL_DISTRIBUTOR_FRAGMENTS: set[str] = {
    "mcmaster", "amazon", "grainger", "home depot", "lowe's",
    "lowes", "ace hardware", "harbor freight", "zoro",
    "fastenal", "bolt depot", "global industrial", "tequipment",
    "uline", "callahan", "autozone",
}


def _is_general_distributor(vendor_name: str) -> bool:
    vn = vendor_name.lower()
    return any(f in vn for f in _GENERAL_DISTRIBUTOR_FRAGMENTS)


# ---------------------------------------------------------------------------
# Signal 5 — Split-vendor handlers (vendors spanning multiple sub-cats)
# ---------------------------------------------------------------------------

def _keyence_classify(desc: str, price: float) -> str:
    dl = desc.lower()
    if any(k in dl for k in ("vhx-", "microscope", "measurement")):
        return SubCat.QUALITY_METROLOGY.value
    if any(k in dl for k in ("sr-x", "sr-", "barcode", "scanner", "gl-v",
                              "gl-r", "sensor", "op-8")):
        return SubCat.CONTROLS_ELECTRICAL.value
    if "shipping" in dl:
        return SubCat.SHIPPING_FREIGHT.value
    if price > 10000:
        return SubCat.QUALITY_METROLOGY.value
    return SubCat.CONTROLS_ELECTRICAL.value


def _precitec_classify(desc: str, _price: float) -> str:
    dl = desc.lower()
    if any(k in dl for k in ("lwm", "sensor unit", "alignment", "pc embedded")):
        return SubCat.PROCESS_EQUIPMENT.value
    if any(k in dl for k in ("lens", "optic", "window", "nozzle", "cover glass")):
        return SubCat.CONSUMABLES.value
    return SubCat.PROCESS_EQUIPMENT.value


def _atlas_copco_classify(desc: str, price: float) -> str:
    dl = desc.lower()
    if any(k in dl for k in ("ilg arm", "torqlift", "torque tool", "nutrunner",
                              "sms-t-", "posi")):
        return SubCat.PROCESS_EQUIPMENT.value
    if any(k in dl for k in ("onsite", "tech support", "commissioning")):
        return SubCat.INTEGRATION_COMMISSIONING.value
    if any(k in dl for k in ("power supply", "power chord", "cable")):
        return SubCat.CONTROLS_ELECTRICAL.value
    if price > 5000:
        return SubCat.PROCESS_EQUIPMENT.value
    return SubCat.CONTROLS_ELECTRICAL.value


def _schmalz_classify(desc: str, price: float) -> str:
    dl = desc.lower()
    if any(k in dl for k in ("gripper", "pump", "fmp-", "area gripper")):
        return SubCat.PROCESS_EQUIPMENT.value
    if any(k in dl for k in ("nipple", "reduction", "sleeve", "hose")):
        return SubCat.MECHANICAL_STRUCTURAL.value
    if price > 1000:
        return SubCat.PROCESS_EQUIPMENT.value
    return SubCat.MECHANICAL_STRUCTURAL.value


def _chroma_classify(desc: str, price: float) -> str:
    dl = desc.lower()
    if any(k in dl for k in ("bi-directional", "power meter", "62060", "66205")):
        return SubCat.PROCESS_EQUIPMENT.value
    if price > 10000:
        return SubCat.PROCESS_EQUIPMENT.value
    return SubCat.CONTROLS_ELECTRICAL.value


def _teguar_classify(_d: str, _p: float) -> str:
    return SubCat.CONTROLS_ELECTRICAL.value


def _centex_classify(desc: str, _price: float) -> str:
    dl = desc.lower()
    if any(k in dl for k in ("crane", "torqlift", "aimco", "lifting")):
        return SubCat.PROCESS_EQUIPMENT.value
    return SubCat.MECHANICAL_STRUCTURAL.value


def _innox_classify(_d: str, _p: float) -> str:
    return SubCat.PROCESS_EQUIPMENT.value


def _ingersoll_classify(desc: str, _price: float) -> str:
    dl = desc.lower()
    if any(k in dl for k in ("compressor", "air dryer", "receiver")):
        return SubCat.FACILITIES_OFFICE.value
    return SubCat.MFG_TOOLS_SUPPLIES.value


def _total_safety_classify(_d: str, _p: float) -> str:
    return SubCat.MFG_TOOLS_SUPPLIES.value


def _bambu_classify(_d: str, _p: float) -> str:
    return SubCat.MFG_TOOLS_SUPPLIES.value


def _acey_classify(desc: str, _price: float) -> str:
    if "shipping" in desc.lower() or "freight" in desc.lower():
        return SubCat.SHIPPING_FREIGHT.value
    return SubCat.PROCESS_EQUIPMENT.value


def _nj_malin_classify(desc: str, _price: float) -> str:
    """N.J. Malin: AGV/material handling integrator.  Sells AGV carts
    (process equip), racking (mechanical), plus PM/design services."""
    dl = desc.lower()
    if any(k in dl for k in ("agv cart", "agv ", "battery module cart")):
        return SubCat.PROCESS_EQUIPMENT.value
    if any(k in dl for k in ("racking", "pallet racking")):
        return SubCat.MECHANICAL_STRUCTURAL.value
    if any(k in dl for k in ("magnetic guide", "magnetic marker", "900mhz",
                              "rfid", "sensor")):
        return SubCat.CONTROLS_ELECTRICAL.value
    if any(k in dl for k in ("project management", "pilot", "concepting",
                              "permitting")):
        return SubCat.DESIGN_ENGINEERING.value
    if _price > 20000:
        return SubCat.PROCESS_EQUIPMENT.value
    return SubCat.DESIGN_ENGINEERING.value


def _ups_classify(desc: str, _price: float) -> str:
    dl = desc.lower()
    if any(k in dl for k in ("battery", "backup", "uninterrupt")):
        return SubCat.CONTROLS_ELECTRICAL.value
    return SubCat.SHIPPING_FREIGHT.value


_SPLIT_VENDORS: dict[str, Any] = {
    "keyence": _keyence_classify,
    "precitec": _precitec_classify,
    "atlas copco": _atlas_copco_classify,
    "schmalz": _schmalz_classify,
    "chroma": _chroma_classify,
    "teguar": _teguar_classify,
    "centex material": _centex_classify,
    "shenzhen innox": _innox_classify,
    "ingersoll rand": _ingersoll_classify,
    "total safety": _total_safety_classify,
    "bambu lab": _bambu_classify,
    "acey technology": _acey_classify,
    "ups": _ups_classify,
    "n.j. malin": _nj_malin_classify,
}


def _split_vendor_classify(vendor: str, desc: str, price: float) -> str | None:
    vn = vendor.lower().strip()
    for fragment, fn in _SPLIT_VENDORS.items():
        if fragment in vn:
            return fn(desc, price)
    return None


# ---------------------------------------------------------------------------
# Signal 6 — Line-item overrides (shipping/services within equipment POs)
# ---------------------------------------------------------------------------

_SERVICE_PAT = re.compile(
    r"(controls?\s+integration|commissioning|installation\b|^install\b"
    r"|on-?site\s+(install|support|tech|fee)|start-?up"
    r"|FAT\b|SAT\b|calibration\s+service|training\b"
    r"|wiring\s+(scope|labor)|electrical\s+(scope|install)"
    r"|^line\s+\d+\s+labor\b|^\w+\s+labor\b)", re.I)

_SHIPPING_PAT = re.compile(
    r"(^shipping\b|^freight\b|^crating\b|^handling\b|^DDP\b"
    r"|shipping\s*(cost|charge|fee|&)"
    r"|unit\s+shipping|^line\s+\d+\s+shipping)", re.I)

_SOFTWARE_PAT = re.compile(
    r"(software\b|license\s+(key|fee)|subscription\b"
    r"|fleet\s+management\s+software)", re.I)

_WARRANTY_PAT = re.compile(
    r"(warranty\b|support\s+package|maintenance\s+contract"
    r"|service\s+agreement|extended\s+support)", re.I)

_DISCOUNT_PAT = re.compile(r"(^discount\b|^rebate\b|credit\s*back)", re.I)


def _line_item_override(desc: str, subtotal: float) -> str | None:
    if not desc:
        return None
    if subtotal < 0 or _DISCOUNT_PAT.search(desc):
        return None
    if _SHIPPING_PAT.search(desc):
        return SubCat.SHIPPING_FREIGHT.value
    if _SERVICE_PAT.search(desc):
        return SubCat.INTEGRATION_COMMISSIONING.value
    if _SOFTWARE_PAT.search(desc):
        return SubCat.SOFTWARE_LICENSES.value
    if _WARRANTY_PAT.search(desc):
        return SubCat.INTEGRATION_COMMISSIONING.value
    return None


# ---------------------------------------------------------------------------
# Master classifier
# ---------------------------------------------------------------------------

def classify_mfg_subcategory(
    vendor_name: str,
    item_description: str,
    line_description: str,
    product_category: str,
    project_name: str,
    price_subtotal: float,
    price_unit: float,
    ramp_card: str,
    source: str,
    line_type: str,
) -> tuple[str, float, str]:
    """Classify a single line item.

    Returns (sub_category, confidence 0-1, reason_string).
    """
    if line_type != "spend":
        return "", 0.0, "non-spend row"

    def _s(v: Any) -> str:
        return str(v).strip() if v and not pd.isna(v) else ""

    def _f(v: Any) -> float:
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return 0.0
        try:
            return float(v)
        except (TypeError, ValueError):
            return 0.0

    desc = _s(item_description)
    ldesc = _s(line_description)
    vendor = _s(vendor_name)
    proj = _s(project_name)
    cat = _s(product_category)
    card = _s(ramp_card)
    subtotal = _f(price_subtotal)
    unit_px = _f(price_unit)

    search_text = desc if desc and desc != "nan" else ldesc
    reasons: list[str] = []

    # Step 1 — Line-item overrides (shipping/services within equipment POs)
    override = _line_item_override(search_text, subtotal)
    if override:
        return override, 0.85, "line-override: {}".format(search_text[:60])

    # Step 2 — Split-vendor classification
    split_result = _split_vendor_classify(vendor, search_text, subtotal)
    if split_result:
        return split_result, 0.80, "split-vendor({})".format(vendor[:30])

    # Step 3 — Specialist vendor
    vendor_result = _vendor_classify(vendor)
    if vendor_result:
        return vendor_result, 0.85, "vendor={}".format(vendor[:30])

    # Step 4 — General distributors: keyword > project > card > default
    if _is_general_distributor(vendor):
        kw_hits = _keyword_classify(search_text)
        if kw_hits:
            return kw_hits[0][0], 0.70, "dist+kw({})".format(kw_hits[0][1][:30])

        proj_hint = _project_hint(proj)
        if proj_hint:
            return proj_hint, 0.65, "dist+proj({})".format(proj[:30])

        card_hint = _ramp_card_hint(card)
        if card_hint:
            return card_hint, 0.60, "dist+card({})".format(card[:30])

        return SubCat.MFG_TOOLS_SUPPLIES.value, 0.50, "dist-default({})".format(vendor[:25])

    # Step 5 — Keyword classification
    kw_hits = _keyword_classify(search_text)
    if kw_hits:
        best = kw_hits[0]
        proj_hint = _project_hint(proj)
        if proj_hint and proj_hint == best[0]:
            return best[0], 0.85, "kw({})+proj-confirms".format(best[1][:30])
        return best[0], 0.75, "kw({})".format(best[1][:40])

    # Step 6 — Project hint
    proj_hint = _project_hint(proj)
    if proj_hint:
        return proj_hint, 0.55, "project-only({})".format(proj[:30])

    # Step 7 — Ramp card hint
    card_hint = _ramp_card_hint(card)
    if card_hint:
        return card_hint, 0.50, "card-only({})".format(card[:30])

    # Step 8 — Existing product_category as fallback
    cat_lower = cat.lower()
    if "r&d services" in cat_lower:
        return SubCat.DESIGN_ENGINEERING.value, 0.45, "fallback-cat: R&D Services"
    if "r&d shipping" in cat_lower:
        return SubCat.SHIPPING_FREIGHT.value, 0.45, "fallback-cat: R&D Shipping"
    if "g&a shipping" in cat_lower:
        return SubCat.SHIPPING_FREIGHT.value, 0.45, "fallback-cat: G&A Shipping"
    if "inbound production shipping" in cat_lower:
        return SubCat.SHIPPING_FREIGHT.value, 0.45, "fallback-cat: Inbound Ship"
    if "software" in cat_lower:
        return SubCat.SOFTWARE_LICENSES.value, 0.45, "fallback-cat: Software"
    if "it equipment" in cat_lower:
        return SubCat.IT_EQUIPMENT.value, 0.45, "fallback-cat: IT Equipment"
    if "office equipment" in cat_lower:
        return SubCat.FACILITIES_OFFICE.value, 0.45, "fallback-cat: Office"
    if "furniture" in cat_lower:
        return SubCat.FACILITIES_OFFICE.value, 0.45, "fallback-cat: Furniture"
    if "shop tooling" in cat_lower:
        return SubCat.MFG_TOOLS_SUPPLIES.value, 0.45, "fallback-cat: Shop Tooling"
    if "deployment tooling" in cat_lower:
        return SubCat.MFG_TOOLS_SUPPLIES.value, 0.45, "fallback-cat: Deploy Tooling"

    # Step 9 — Price heuristic
    if unit_px >= 50000 or subtotal >= 50000:
        return SubCat.PROCESS_EQUIPMENT.value, 0.40, "price(>50k)"
    if unit_px >= 5000 or subtotal >= 5000:
        return SubCat.PROCESS_EQUIPMENT.value, 0.35, "price(>5k)"

    # Step 10 — Fallback
    if source == "ramp":
        return SubCat.MFG_TOOLS_SUPPLIES.value, 0.30, "fallback: ramp"
    return SubCat.MFG_TOOLS_SUPPLIES.value, 0.25, "fallback: no-signals"


# ---------------------------------------------------------------------------
# Batch classifier
# ---------------------------------------------------------------------------

def classify_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Add mfg_subcategory, subcat_confidence, subcat_reason, is_mfg."""
    out = df.copy()

    subcats: list[str] = []
    confs: list[float] = []
    reasons_list: list[str] = []

    for _, row in out.iterrows():
        sc, conf, reason = classify_mfg_subcategory(
            vendor_name=row.get("vendor_name", ""),
            item_description=row.get("item_description", ""),
            line_description=row.get("line_description", ""),
            product_category=row.get("product_category", ""),
            project_name=row.get("project_name", ""),
            price_subtotal=row.get("price_subtotal", 0),
            price_unit=row.get("price_unit", 0),
            ramp_card=row.get("ramp_card", ""),
            source=row.get("source", "odoo"),
            line_type=row.get("line_type", "spend"),
        )
        subcats.append(sc)
        confs.append(conf)
        reasons_list.append(reason)

    out["mfg_subcategory"] = subcats
    out["subcat_confidence"] = confs
    out["subcat_reason"] = reasons_list
    out["is_mfg"] = out["mfg_subcategory"].isin(MFG_SUBCATS)

    return out


def rescrub_low_confidence_subcategories(
    df: pd.DataFrame,
    confidence_threshold: float = 0.6,
) -> pd.DataFrame:
    """Re-run subcategory classification only on rows with low subcat_confidence.

    Use after rule or data updates to refresh labels for previously uncertain rows.
    Rows with subcat_confidence >= threshold (or non-spend) are left unchanged.
    """
    spend = df["line_type"] == "spend"
    conf_col = df.get("subcat_confidence")
    if conf_col is None:
        return df
    low_conf = pd.to_numeric(conf_col, errors="coerce").fillna(0) < confidence_threshold
    mask = spend & low_conf

    if not mask.any():
        return df

    subset = df.loc[mask].copy()
    reclassified = classify_dataframe(subset)
    out = df.copy()
    for col in ("mfg_subcategory", "subcat_confidence", "subcat_reason"):
        out.loc[mask, col] = reclassified[col].values
    out["is_mfg"] = out["mfg_subcategory"].isin(MFG_SUBCATS)
    return out
