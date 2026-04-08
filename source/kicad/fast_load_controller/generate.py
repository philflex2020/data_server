#!/usr/bin/env python3
"""
Generate KiCad 8 schematic and PCB for the fast_load_controller board.

Circuit:
  J1 (12V barrel) → U2 (12V→5V buck) → U1 (Heltec LoRa32 V2, 3.3V reg on-board)
                  → U3 (12V→15V boost) → U4 pin 4 (V+) / pin 11 (V-)=GND

  GPIO25 → R1(100Ω) → OA1 buffer (U4-A) → OA2 amp (U4-B) → J2 pin1 (DAC1_OUT 0-11.5V)
  GPIO26 → R2(100Ω) → OA3 buffer (U4-C) → OA4 amp (U4-D) → J3 pin1 (DAC2_OUT 0-11.5V)

  Gain stage: R_A=3.3kΩ (–in to GND), R_B=8.2kΩ (output to –in), gain = 1 + 8.2/3.3 = 3.485×
  Note: GPIO25 may conflict with onboard LED on some Heltec V2 revisions.

Run:  python3 generate.py
Output: fast_load_controller.kicad_sch
        fast_load_controller.kicad_pcb
        fast_load_controller.kicad_pro
"""

import math, os

OUT_DIR = os.path.dirname(os.path.abspath(__file__))

def _pin(etype, x, y, angle, length, name, number):
    """Generate a correctly-formatted KiCad 8 pin S-expression."""
    return (f'        (pin {etype} line (at {x} {y} {angle}) (length {length})\n'
            f'          (name "{name}" (effects (font (size 1.27 1.27))))\n'
            f'          (number "{number}" (effects (font (size 1.27 1.27))))\n'
            f'        )')


def sch_sym_lib():
    """Embedded symbol definitions for all custom/module components."""

    def prop(name, val, x=0, y=0, hide=False, angle=0):
        h = " hide" if hide else ""
        return (f'      (property "{name}" "{val}" (at {x} {y} {angle})\n'
                f'        (effects (font (size 1.27 1.27)){h})\n'
                f'      )')

    def sym_hdr(libname, symname, pin_names_offset=1.016, in_bom=True, on_board=True, power=False):
        bom = "yes" if in_bom else "no"
        ob  = "yes" if on_board else "no"
        pwr = "\n      (power)" if power else ""
        return (f'    (symbol "{libname}:{symname}"{pwr}\n'
                f'      (pin_names (offset {pin_names_offset}))\n'
                f'      (in_bom {bom}) (on_board {ob})')

    def polyline(pts_list, stroke_w=0, fill="none"):
        """Multi-line polyline matching real KiCad format."""
        xy_lines = "\n".join(f"            (xy {x} {y})" for x, y in pts_list)
        return (f'        (polyline\n'
                f'          (pts\n'
                f'{xy_lines}\n'
                f'          )\n'
                f'          (stroke (width {stroke_w}) (type default))\n'
                f'          (fill (type {fill}))\n'
                f'        )')

    def power_pin_hidden(x, y, angle, length, name, number):
        """Power supply pin with hide flag (standard KiCad convention)."""
        return (f'        (pin power_in line (at {x} {y} {angle}) (length {length}) hide\n'
                f'          (name "{name}" (effects (font (size 1.27 1.27))))\n'
                f'          (number "{number}" (effects (font (size 1.27 1.27))))\n'
                f'        )')

    parts = ['  (lib_symbols']

    # ── J1 BarrelJack ────────────────────────────────────────────────────────
    parts += [
        sym_hdr("FastLoad", "BarrelJack"),
        prop("Reference", "J", x=0, y=3.81),
        prop("Value", "BarrelJack_12V", x=0, y=-3.81),
        prop("Footprint", "Connector_BarrelJack:BarrelJack_CUI_PJ-002AH", hide=True),
        prop("Datasheet", "~", hide=True),
        '      (symbol "BarrelJack_1_1"',
        '        (rectangle (start -2.54 -2.54) (end 2.54 2.54) (stroke (width 0) (type default)) (fill (type none)))',
        _pin("power_in", -5.08, 1.27, 0, 2.54, "+", "1"),
        _pin("power_in", -5.08, -1.27, 0, 2.54, "-", "2"),
        '      )',
        '    )',
    ]

    # ── U2 Buck 12V→5V ───────────────────────────────────────────────────────
    parts += [
        sym_hdr("FastLoad", "BuckModule"),
        prop("Reference", "U", x=0, y=6.35),
        prop("Value", "Buck_12V_5V", x=0, y=-6.35),
        prop("Footprint", "FastLoad:BuckModule_SIP4", hide=True),
        prop("Datasheet", "~", hide=True),
        '      (symbol "BuckModule_1_1"',
        '        (rectangle (start -5.08 -5.08) (end 5.08 5.08) (stroke (width 0) (type default)) (fill (type background)))',
        '        (text "DC-DC" (at 0 1.27 0) (effects (font (size 1.27 1.27))))',
        '        (text "12V-5V" (at 0 -1.27 0) (effects (font (size 1.27 1.27))))',
        _pin("power_in",  -7.62,  2.54, 0,   2.54, "VIN",     "1"),
        _pin("power_in",  -7.62, -2.54, 0,   2.54, "GND_IN",  "2"),
        _pin("power_out",  7.62,  2.54, 180, 2.54, "VOUT",    "3"),
        _pin("power_out",  7.62, -2.54, 180, 2.54, "GND_OUT", "4"),
        '      )',
        '    )',
    ]

    # ── U3 Boost 12V→15V ─────────────────────────────────────────────────────
    parts += [
        sym_hdr("FastLoad", "BoostModule"),
        prop("Reference", "U", x=0, y=6.35),
        prop("Value", "Boost_12V_15V", x=0, y=-6.35),
        prop("Footprint", "FastLoad:BoostModule_SIP4", hide=True),
        prop("Datasheet", "~", hide=True),
        '      (symbol "BoostModule_1_1"',
        '        (rectangle (start -5.08 -5.08) (end 5.08 5.08) (stroke (width 0) (type default)) (fill (type background)))',
        '        (text "DC-DC" (at 0 1.27 0) (effects (font (size 1.27 1.27))))',
        '        (text "12V-15V" (at 0 -1.27 0) (effects (font (size 1.27 1.27))))',
        _pin("power_in",  -7.62,  2.54, 0,   2.54, "VIN",     "1"),
        _pin("power_in",  -7.62, -2.54, 0,   2.54, "GND_IN",  "2"),
        _pin("power_out",  7.62,  2.54, 180, 2.54, "VOUT",    "3"),
        _pin("power_out",  7.62, -2.54, 180, 2.54, "GND_OUT", "4"),
        '      )',
        '    )',
    ]

    # ── U1 Heltec LoRa32 V2 ──────────────────────────────────────────────────
    lora_pins = [
        ("power_in",     -12.7,  17.78,  0,   2.54, "5V",           "1"),
        ("power_in",     -12.7,  15.24,  0,   2.54, "GND",          "2"),
        ("output",        12.7,  17.78, 180,  2.54, "GPIO25",        "3"),
        ("output",        12.7,  15.24, 180,  2.54, "GPIO26",        "4"),
        ("bidirectional", 12.7,  12.7,  180,  2.54, "GPIO0",         "5"),
        ("bidirectional", 12.7,  10.16, 180,  2.54, "GPIO4",         "6"),
        ("bidirectional", 12.7,   7.62, 180,  2.54, "GPIO5_SPI_CS",  "7"),
        ("bidirectional", 12.7,   5.08, 180,  2.54, "GPIO14",        "8"),
        ("bidirectional", 12.7,   2.54, 180,  2.54, "GPIO18_SPI_CLK","9"),
        ("bidirectional", 12.7,   0,    180,  2.54, "GPIO19_SPI_MISO","10"),
        ("bidirectional", 12.7,  -2.54, 180,  2.54, "GPIO23_SPI_MOSI","11"),
        ("bidirectional",-12.7,  12.7,   0,   2.54, "GPIO13",        "12"),
        ("bidirectional",-12.7,  10.16,  0,   2.54, "GPIO12",        "13"),
        ("bidirectional",-12.7,   7.62,  0,   2.54, "GPIO17",        "14"),
        ("bidirectional",-12.7,   5.08,  0,   2.54, "GPIO16",        "15"),
        ("bidirectional",-12.7,   2.54,  0,   2.54, "GPIO21_SDA",    "16"),
        ("bidirectional",-12.7,   0,     0,   2.54, "GPIO22_SCL",    "17"),
        ("bidirectional",-12.7,  -2.54,  0,   2.54, "GPIO36_VP",     "18"),
        ("bidirectional",-12.7,  -5.08,  0,   2.54, "GPIO37_VN",     "19"),
        ("power_out",    -12.7, -10.16,  0,   2.54, "3V3_OUT",       "20"),
    ]
    parts += [
        sym_hdr("FastLoad", "LoRa32V2"),
        prop("Reference", "U", x=0, y=22.86),
        prop("Value", "HeltecLoRa32V2", x=0, y=-22.86),
        prop("Footprint", "FastLoad:HeltecLoRa32V2_Module", hide=True),
        prop("Datasheet", "~", hide=True),
        '      (symbol "LoRa32V2_1_1"',
        '        (rectangle (start -10.16 -20.32) (end 10.16 20.32) (stroke (width 0) (type default)) (fill (type background)))',
        '        (text "Heltec LoRa32 V2" (at 0 0 0) (effects (font (size 1.27 1.27))))',
    ] + [_pin(*p) for p in lora_pins] + [
        '      )',
        '    )',
    ]

    # ── U4 LM324 ─────────────────────────────────────────────────────────────
    lm_pins = [
        ("output",    10.16,   5.08, 180, 2.54, "OUT_A", "1"),
        ("input",    -10.16,   3.81,   0, 2.54, "-IN_A", "2"),
        ("input",    -10.16,   6.35,   0, 2.54, "+IN_A", "3"),
        ("power_in",   0,     11.43, 270, 2.54, "V+",    "4"),
        ("input",    -10.16,   1.27,   0, 2.54, "+IN_B", "5"),
        ("input",    -10.16,  -1.27,   0, 2.54, "-IN_B", "6"),
        ("output",    10.16,   0,    180, 2.54, "OUT_B", "7"),
        ("output",    10.16,  -5.08, 180, 2.54, "OUT_C", "8"),
        ("input",    -10.16,  -6.35,   0, 2.54, "-IN_C", "9"),
        ("input",    -10.16,  -3.81,   0, 2.54, "+IN_C","10"),
        ("power_in",   0,    -13.97,  90, 2.54, "GND",  "11"),
        ("input",    -10.16,  -8.89,   0, 2.54, "+IN_D","12"),
        ("input",    -10.16, -11.43,   0, 2.54, "-IN_D","13"),
        ("output",    10.16, -10.16, 180, 2.54, "OUT_D","14"),
    ]
    parts += [
        sym_hdr("FastLoad", "LM324"),
        prop("Reference", "U", x=0, y=10.16),
        prop("Value", "LM324", x=0, y=-10.16),
        prop("Footprint", "Package_DIP:DIP-14_W7.62mm", hide=True),
        prop("Datasheet", "~", hide=True),
        '      (symbol "LM324_1_1"',
        '        (rectangle (start -7.62 -13.97) (end 7.62 8.89) (stroke (width 0) (type default)) (fill (type background)))',
        '        (text "LM324" (at 0 -2.54 0) (effects (font (size 1.27 1.27))))',
    ] + [_pin(*p) for p in lm_pins] + [
        '      )',
        '    )',
    ]

    # ── Device:R ─────────────────────────────────────────────────────────────
    parts += [
        '    (symbol "Device:R"',
        '      (pin_names (offset 0))',
        '      (in_bom yes) (on_board yes)',
        prop("Reference", "R", x=1.524, y=0, angle=90),
        prop("Value",     "R", x=-1.524, y=0, angle=90),
        prop("Footprint", "", hide=True),
        prop("Datasheet", "~", hide=True),
        '      (symbol "R_0_1"',
        '        (rectangle (start -1.016 -2.032) (end 1.016 2.032) (stroke (width 0) (type default)) (fill (type none)))',
        '      )',
        '      (symbol "R_1_1"',
        _pin("passive", 0,  3.81, 270, 1.778, "~", "1"),
        _pin("passive", 0, -3.81,  90, 1.778, "~", "2"),
        '      )',
        '    )',
    ]

    # ── Device:C ─────────────────────────────────────────────────────────────
    parts += [
        '    (symbol "Device:C"',
        '      (pin_names (offset 0.254))',
        '      (in_bom yes) (on_board yes)',
        prop("Reference", "C", x=1.524, y=0, angle=90),
        prop("Value",     "C", x=-1.524, y=0, angle=90),
        prop("Footprint", "", hide=True),
        prop("Datasheet", "~", hide=True),
        '      (symbol "C_0_1"',
        polyline([(-2.032, 0.508), (2.032, 0.508)], stroke_w=0.508),
        polyline([(-2.032, -0.508), (2.032, -0.508)], stroke_w=0.508),
        '      )',
        '      (symbol "C_1_1"',
        _pin("passive", 0,  3.81, 270, 3.302, "~", "1"),
        _pin("passive", 0, -3.81,  90, 3.302, "~", "2"),
        '      )',
        '    )',
    ]

    # ── Connector_Generic:Conn_01x02 ─────────────────────────────────────────
    parts += [
        '    (symbol "Connector_Generic:Conn_01x02"',
        '      (pin_names (offset 1.016))',
        '      (in_bom yes) (on_board yes)',
        prop("Reference", "J", x=0, y=5.08),
        prop("Value", "Conn_01x02", x=0, y=-5.08),
        prop("Footprint", "Connector_PinHeader_2.54mm:PinHeader_1x02_P2.54mm_Vertical", hide=True),
        prop("Datasheet", "~", hide=True),
        '      (symbol "Conn_01x02_1_1"',
        '        (rectangle (start -1.27 -3.81) (end 1.27 3.81) (stroke (width 0) (type default)) (fill (type background)))',
        _pin("passive", -3.81,  2.54, 0, 2.54, "Pin_1", "1"),
        _pin("passive", -3.81,  0,    0, 2.54, "Pin_2", "2"),
        '      )',
        '    )',
    ]

    # ── power:GND ────────────────────────────────────────────────────────────
    parts += [
        '    (symbol "power:GND" (power) (pin_names (offset 0)) (in_bom yes) (on_board yes)',
        prop("Reference", "#PWR", x=0, y=-6.35, hide=True),
        prop("Value", "GND", x=0, y=-3.81),
        prop("Footprint", "", hide=True),
        prop("Datasheet", "", hide=True),
        '      (symbol "GND_0_1"',
        polyline([(0,0),(0,-1.27),(1.27,-1.27),(0,-2.54),(-1.27,-1.27),(0,-1.27)]),
        '      )',
        '      (symbol "GND_1_1"',
        power_pin_hidden(0, 0, 270, 0, "GND", "1"),
        '      )',
        '    )',
    ]

    def power_rail(name, val):
        return [
            f'    (symbol "power:{name}" (power) (pin_names (offset 0)) (in_bom yes) (on_board yes)',
            prop("Reference", "#PWR", x=0, y=5.08, hide=True),
            prop("Value", val, x=0, y=3.556),
            prop("Footprint", "", hide=True),
            prop("Datasheet", "", hide=True),
            f'      (symbol "{name}_0_1"',
            polyline([(0, 0), (0, 2.54)]),
            polyline([(-1.27, 2.54), (0, 4.064), (1.27, 2.54)]),
            '      )',
            f'      (symbol "{name}_1_1"',
            power_pin_hidden(0, 0, 90, 0, name, "1"),
            '      )',
            '    )',
        ]

    parts += power_rail("+5V",  "+5V")
    parts += power_rail("+12V", "+12V")
    parts += power_rail("+15V", "+15V")

    parts.append('  )')
    return "\n".join(parts)

def make_uuid(seed):
    """Deterministic fake UUID from a seed string."""
    import hashlib
    h = hashlib.md5(seed.encode()).hexdigest()
    return f"{h[0:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:32]}"


def sym_instance(lib, sym, ref, val, x, y, angle=0, props=None, unit=1, fp=""):
    uuid = make_uuid(f"{ref}{x}{y}")
    angle_str = str(float(angle))
    lines = [
        f'  (symbol (lib_id "{lib}:{sym}")',
        f'    (at {x} {y} {angle_str})',
        f'    (unit {unit})',
        f'    (in_bom yes) (on_board yes)',
        f'    (uuid "{uuid}")',
        f'    (property "Reference" "{ref}" (at {x} {y-3.81} 0) (effects (font (size 1.27 1.27))))',
        f'    (property "Value" "{val}" (at {x} {y+2.54} 0) (effects (font (size 1.27 1.27))))',
    ]
    if fp:
        lines.append(f'    (property "Footprint" "{fp}" (at {x} {y} 0) (effects (font (size 1.27 1.27)) hide))')
    if props:
        for k, v in props.items():
            lines.append(f'    (property "{k}" "{v}" (at {x} {y} 0) (effects (font (size 1.27 1.27)) hide))')
    lines.append('  )')
    return "\n".join(lines)


def wire(x1, y1, x2, y2):
    uuid = make_uuid(f"w{x1}{y1}{x2}{y2}")
    return (f'  (wire (pts (xy {x1} {y1}) (xy {x2} {y2}))\n'
            f'    (stroke (width 0) (type default))\n'
            f'    (uuid "{uuid}")\n'
            f'  )')


def net_label(text, x, y, angle=0):
    uuid = make_uuid(f"nl{text}{x}{y}")
    return (f'  (label "{text}" (at {x} {y} {angle}) (fields_autoplaced)\n'
            f'    (effects (font (size 1.27 1.27)) (justify left bottom))\n'
            f'    (uuid "{uuid}")\n'
            f'  )')


def text_note(txt, x, y, size=1.27, bold=False):
    uuid = make_uuid(f"txt{txt}{x}{y}")
    bold_str = " bold" if bold else ""
    return (f'  (text "{txt}" (at {x} {y} 0)\n'
            f'    (effects (font (size {size} {size}){bold_str}))\n'
            f'    (uuid "{uuid}")\n'
            f'  )')


def generate_schematic():
    lines = []
    lines.append('(kicad_sch')
    lines.append('  (version 20231120)')
    lines.append('  (generator "fast_load_generate")')
    lines.append('  (generator_version "1.0")')
    lines.append(f'  (paper "A3")')
    lines.append(f'  (uuid "{make_uuid("fast_load_controller_sch")}")')
    lines.append('')

    lines.append(sch_sym_lib())
    lines.append('')

    # ── Symbols ─────────────────────────────────────────────────────────────
    # J1 barrel jack (top-left)
    lines.append(sym_instance("FastLoad", "BarrelJack", "J1", "BarrelJack_12V", 20, 30,
                               fp="Connector_BarrelJack:BarrelJack_CUI_PJ-002AH"))
    # U2 buck
    lines.append(sym_instance("FastLoad", "BuckModule", "U2", "Buck_12V_5V", 60, 30,
                               fp="FastLoad:BuckModule_SIP4"))
    # U3 boost
    lines.append(sym_instance("FastLoad", "BoostModule", "U3", "Boost_12V_15V", 60, 60,
                               fp="FastLoad:BoostModule_SIP4"))
    # U1 LoRa32
    lines.append(sym_instance("FastLoad", "LoRa32V2", "U1", "HeltecLoRa32V2", 110, 80,
                               fp="FastLoad:HeltecLoRa32V2_Module"))
    # U4 LM324
    lines.append(sym_instance("FastLoad", "LM324", "U4", "LM324", 175, 80,
                               fp="Package_DIP:DIP-14_W7.62mm"))

    # Resistors: R1,R2 series (100Ω), R3,R4 R_A (3.3kΩ), R5,R6 R_B (8.2kΩ)
    lines.append(sym_instance("Device", "R", "R1", "100", 148, 95,
                               fp="Resistor_THT:R_Axial_DIN0207_L6.3mm_D2.5mm_P7.62mm_Horizontal"))
    lines.append(sym_instance("Device", "R", "R2", "100", 148, 115,
                               fp="Resistor_THT:R_Axial_DIN0207_L6.3mm_D2.5mm_P7.62mm_Horizontal"))
    lines.append(sym_instance("Device", "R", "R3", "3.3k", 185, 100,   # R_A ch1
                               fp="Resistor_THT:R_Axial_DIN0207_L6.3mm_D2.5mm_P7.62mm_Horizontal"))
    lines.append(sym_instance("Device", "R", "R4", "3.3k", 185, 120,   # R_A ch2
                               fp="Resistor_THT:R_Axial_DIN0207_L6.3mm_D2.5mm_P7.62mm_Horizontal"))
    lines.append(sym_instance("Device", "R", "R5", "8.2k", 200, 100,   # R_B ch1
                               fp="Resistor_THT:R_Axial_DIN0207_L6.3mm_D2.5mm_P7.62mm_Horizontal"))
    lines.append(sym_instance("Device", "R", "R6", "8.2k", 200, 120,   # R_B ch2
                               fp="Resistor_THT:R_Axial_DIN0207_L6.3mm_D2.5mm_P7.62mm_Horizontal"))

    # Decoupling capacitors
    lines.append(sym_instance("Device", "C", "C1", "100nF", 70, 25,
                               fp="Capacitor_THT:C_Disc_D5.0mm_W2.5mm_P5.00mm"))
    lines.append(sym_instance("Device", "C", "C2", "100nF", 70, 55,
                               fp="Capacitor_THT:C_Disc_D5.0mm_W2.5mm_P5.00mm"))
    lines.append(sym_instance("Device", "C", "C3", "10uF", 125, 45,
                               fp="Capacitor_THT:CP_Radial_D5.0mm_P2.50mm"))
    lines.append(sym_instance("Device", "C", "C4", "100nF", 185, 75,
                               fp="Capacitor_THT:C_Disc_D5.0mm_W2.5mm_P5.00mm"))

    # Output connectors J2 (DAC1), J3 (DAC2)
    lines.append(sym_instance("Connector_Generic", "Conn_01x02", "J2", "DAC1_OUT", 230, 95,
                               fp="Connector_PinHeader_2.54mm:PinHeader_1x02_P2.54mm_Vertical"))
    lines.append(sym_instance("Connector_Generic", "Conn_01x02", "J3", "DAC2_OUT", 230, 115,
                               fp="Connector_PinHeader_2.54mm:PinHeader_1x02_P2.54mm_Vertical"))

    # Power flags
    lines.append(sym_instance("power", "GND", "#PWR01", "GND", 20, 38))
    lines.append(sym_instance("power", "+12V", "#PWR02", "+12V", 20, 22))
    lines.append(sym_instance("power", "+5V",  "#PWR03", "+5V",  80, 22))
    lines.append(sym_instance("power", "+15V", "#PWR04", "+15V", 80, 52))
    lines.append(sym_instance("power", "GND",  "#PWR05", "GND",  60, 40))
    lines.append(sym_instance("power", "GND",  "#PWR06", "GND",  60, 70))

    lines.append('')

    # ── Net labels ──────────────────────────────────────────────────────────
    lines.append(net_label("DAC1_BUF", 155, 93))
    lines.append(net_label("DAC1_OUT", 225, 93))
    lines.append(net_label("DAC2_BUF", 155, 113))
    lines.append(net_label("DAC2_OUT", 225, 113))

    lines.append('')

    # ── Annotation notes ────────────────────────────────────────────────────
    lines.append(text_note("!!! GPIO25 may conflict with onboard LED on some Heltec V2 revisions.", 105, 60, size=1.0, bold=True))
    lines.append(text_note("Use GPIO26 (DAC2) if LED interference is observed.", 105, 62, size=1.0))
    lines.append(text_note("Gain = 1 + R_B/R_A = 1 + 8.2k/3.3k = 3.485x  => 0-3.3V -> 0-11.5V", 105, 66, size=1.0))
    lines.append(text_note("R1/R2 = 100 ohm DAC protection series resistors", 105, 68, size=1.0))
    lines.append(text_note("OA-A,C = unity-gain buffer   OA-B,D = non-inverting amp", 105, 70, size=1.0))
    lines.append(text_note("U4 V+ = +15V (pin 4),  V- = GND (pin 11)", 105, 72, size=1.0))

    lines.append('')
    lines.append('  (title_block')
    lines.append('    (title "Fast Load Controller - DAC Output Stage")')
    lines.append('    (date "2026-04-06")')
    lines.append('    (rev "1.0")')
    lines.append('    (company "Heltec LoRa32 V2 + LM324 Dual-Channel DAC Amplifier")')
    lines.append('  )')
    lines.append('')
    lines.append(')')

    return "\n".join(lines)


def generate_pcb():
    """Generate KiCad 8 PCB file with component placement."""

    def fp_ref(ref, x, y, layer="F.Cu"):
        return (f'  (footprint "FastLoad:{ref}"\n'
                f'    (layer "{layer}")\n'
                f'    (at {x} {y})\n'
                f'    (property "Reference" "{ref}" (at 0 -3 0) (layer "F.Fab")\n'
                f'      (effects (font (size 1 1))))\n'
                f'  )')

    def th_pad(num, x, y, drill=1.0, size=1.6):
        uuid = make_uuid(f"pad{num}{x}{y}")
        return (f'      (pad "{num}" thru_hole circle (at {x} {y}) (size {size} {size})\n'
                f'        (drill {drill}) (layers "*.Cu" "*.Mask")\n'
                f'        (uuid "{uuid}"))')

    lines = []
    lines.append('(kicad_pcb')
    lines.append('  (version 20231120)')
    lines.append('  (generator "fast_load_generate")')
    lines.append('  (generator_version "1.0")')
    lines.append('')
    lines.append('  (general')
    lines.append('    (thickness 1.6)')
    lines.append('    (legacy_teardrops no)')
    lines.append('  )')
    lines.append('')
    lines.append('  (paper "A4")')
    lines.append('')
    lines.append('  (layers')
    for lnum, lname, ltype in [
        (0, "F.Cu", "signal"), (31, "B.Cu", "signal"),
        (32, "B.Adhes", "user"), (33, "F.Adhes", "user"),
        (34, "B.Paste", "user"), (35, "F.Paste", "user"),
        (36, "B.SilkS", "user"), (37, "F.SilkS", "user"),
        (38, "B.Mask", "user"), (39, "F.Mask", "user"),
        (40, "Dwgs.User", "user"), (44, "Edge.Cuts", "user"),
        (45, "Margin", "user"), (49, "F.Fab", "user"), (48, "B.Fab", "user"),
    ]:
        lines.append(f'    ({lnum} "{lname}" {ltype})')
    lines.append('  )')
    lines.append('')
    lines.append('  (setup')
    lines.append('    (pad_to_mask_clearance 0.051)')
    lines.append('  )')
    lines.append('')

    # ── Board outline 120×80mm ───────────────────────────────────────────────
    bx, by, bw, bh = 10, 10, 120, 80
    uuid_outline = make_uuid("outline")
    for x1, y1, x2, y2 in [
        (bx, by, bx+bw, by), (bx+bw, by, bx+bw, by+bh),
        (bx+bw, by+bh, bx, by+bh), (bx, by+bh, bx, by)
    ]:
        uid = make_uuid(f"edge{x1}{y1}{x2}{y2}")
        lines.append(f'  (gr_line (start {x1} {y1}) (end {x2} {y2})'
                     f' (stroke (width 0.05) (type solid)) (layer "Edge.Cuts") (uuid "{uid}"))')
    lines.append('')

    # ── Component footprints ─────────────────────────────────────────────────
    # Component positions (mm from top-left of board, origin at 0,0)
    # J1 barrel jack: 16mm, 25mm
    j1x, j1y = 16, 25
    uid = make_uuid(f"J1fp")
    lines.append(f'  (footprint "Connector_BarrelJack:BarrelJack_CUI_PJ-002AH" (layer "F.Cu") (at {j1x} {j1y})')
    lines.append(f'    (property "Reference" "J1" (at 0 -4 0) (layer "F.Fab") (effects (font (size 1 1))))')
    lines.append(f'    (property "Value" "BarrelJack_12V" (at 0 4 0) (layer "F.Fab") (effects (font (size 1 1))))')
    lines.append(f'    (uuid "{uid}")')
    lines.append(th_pad("1", 0, -1.6, drill=1.2, size=2.0))
    lines.append(th_pad("2", 0, 1.6, drill=1.2, size=2.0))
    lines.append('  )')
    lines.append('')

    # U2 buck module: SIP-4, 2.54mm pitch, 30mm, 25mm
    u2x, u2y = 35, 20
    uid = make_uuid("U2fp")
    lines.append(f'  (footprint "FastLoad:BuckModule_SIP4" (layer "F.Cu") (at {u2x} {u2y})')
    lines.append(f'    (property "Reference" "U2" (at 0 -5 0) (layer "F.Fab") (effects (font (size 1 1))))')
    lines.append(f'    (property "Value" "Buck_12V_5V" (at 0 5 0) (layer "F.Fab") (effects (font (size 1 1))))')
    lines.append(f'    (uuid "{uid}")')
    for i, offset in enumerate([-3.81, -1.27, 1.27, 3.81]):
        lines.append(th_pad(str(i+1), 0, offset, drill=1.0, size=1.8))
    lines.append('  )')
    lines.append('')

    # U3 boost module: SIP-4, 35mm, 55mm
    u3x, u3y = 35, 55
    uid = make_uuid("U3fp")
    lines.append(f'  (footprint "FastLoad:BoostModule_SIP4" (layer "F.Cu") (at {u3x} {u3y})')
    lines.append(f'    (property "Reference" "U3" (at 0 -5 0) (layer "F.Fab") (effects (font (size 1 1))))')
    lines.append(f'    (property "Value" "Boost_12V_15V" (at 0 5 0) (layer "F.Fab") (effects (font (size 1 1))))')
    lines.append(f'    (uuid "{uid}")')
    for i, offset in enumerate([-3.81, -1.27, 1.27, 3.81]):
        lines.append(th_pad(str(i+1), 0, offset, drill=1.0, size=1.8))
    lines.append('  )')
    lines.append('')

    # U1 LoRa32 V2 module headers: two rows of 18 pins, 2.54mm, 23.3mm wide
    u1x, u1y = 70, 45
    uid = make_uuid("U1fp")
    lines.append(f'  (footprint "FastLoad:HeltecLoRa32V2_Module" (layer "F.Cu") (at {u1x} {u1y})')
    lines.append(f'    (property "Reference" "U1" (at 0 -12 0) (layer "F.Fab") (effects (font (size 1 1))))')
    lines.append(f'    (property "Value" "HeltecLoRa32V2" (at 0 12 0) (layer "F.Fab") (effects (font (size 1 1))))')
    lines.append(f'    (uuid "{uid}")')
    # Left row (pins 1-20) at x=-11.65mm, y stepped
    for i in range(20):
        lines.append(th_pad(str(i+1), -11.65, (i - 9.5) * 2.54, drill=1.0, size=1.8))
    # Right row (mirrored, pins 21-40)
    for i in range(20):
        lines.append(th_pad(str(i+21), 11.65, (i - 9.5) * 2.54, drill=1.0, size=1.8))
    lines.append('  )')
    lines.append('')

    # U4 LM324 DIP-14: 7.62mm row spacing, 2.54mm pin pitch
    u4x, u4y = 100, 45
    uid = make_uuid("U4fp")
    lines.append(f'  (footprint "Package_DIP:DIP-14_W7.62mm" (layer "F.Cu") (at {u4x} {u4y})')
    lines.append(f'    (property "Reference" "U4" (at 0 -10 0) (layer "F.Fab") (effects (font (size 1 1))))')
    lines.append(f'    (property "Value" "LM324" (at 0 10 0) (layer "F.Fab") (effects (font (size 1 1))))')
    lines.append(f'    (uuid "{uid}")')
    # DIP-14: left pins 1-7 top to bottom at x=-3.81, right pins 8-14 bottom to top at x=+3.81
    for i in range(7):
        lines.append(th_pad(str(i+1), -3.81, (i - 3) * 2.54, drill=0.8, size=1.6))
    for i in range(7):
        lines.append(th_pad(str(14-i), 3.81, (i - 3) * 2.54, drill=0.8, size=1.6))
    lines.append('  )')
    lines.append('')

    # R1-R6 axial resistors (horizontal, 7.62mm pitch)
    resistors = [
        ("R1", "100", 120, 35),
        ("R2", "100", 120, 55),
        ("R3", "3.3k", 108, 40),
        ("R4", "3.3k", 108, 60),
        ("R5", "8.2k", 120, 40),
        ("R6", "8.2k", 120, 60),
    ]
    for ref, val, rx, ry in resistors:
        uid = make_uuid(f"{ref}fp")
        lines.append(f'  (footprint "Resistor_THT:R_Axial_DIN0207_L6.3mm_D2.5mm_P7.62mm_Horizontal"'
                     f' (layer "F.Cu") (at {rx} {ry})')
        lines.append(f'    (property "Reference" "{ref}" (at 0 -2 0) (layer "F.Fab") (effects (font (size 1 1))))')
        lines.append(f'    (property "Value" "{val}" (at 0 2 0) (layer "F.Fab") (effects (font (size 1 1))))')
        lines.append(f'    (uuid "{uid}")')
        lines.append(th_pad("1", -3.81, 0, drill=0.8, size=1.6))
        lines.append(th_pad("2", 3.81, 0, drill=0.8, size=1.6))
        lines.append('  )')
        lines.append('')

    # C1-C4 capacitors
    caps = [
        ("C1", "100nF", 45, 18),
        ("C2", "100nF", 45, 48),
        ("C3", "10uF", 60, 30),
        ("C4", "100nF", 95, 35),
    ]
    for ref, val, cx, cy in caps:
        uid = make_uuid(f"{ref}fp")
        lines.append(f'  (footprint "Capacitor_THT:C_Disc_D5.0mm_W2.5mm_P5.00mm"'
                     f' (layer "F.Cu") (at {cx} {cy})')
        lines.append(f'    (property "Reference" "{ref}" (at 0 -3 0) (layer "F.Fab") (effects (font (size 1 1))))')
        lines.append(f'    (property "Value" "{val}" (at 0 3 0) (layer "F.Fab") (effects (font (size 1 1))))')
        lines.append(f'    (uuid "{uid}")')
        lines.append(th_pad("1", -2.5, 0, drill=0.8, size=1.6))
        lines.append(th_pad("2",  2.5, 0, drill=0.8, size=1.6))
        lines.append('  )')
        lines.append('')

    # J2 DAC1_OUT connector
    uid = make_uuid("J2fp")
    lines.append(f'  (footprint "Connector_PinHeader_2.54mm:PinHeader_1x02_P2.54mm_Vertical"'
                 f' (layer "F.Cu") (at 115 40))')
    lines.append(f'    (property "Reference" "J2" (at 0 -3 0) (layer "F.Fab") (effects (font (size 1 1))))')
    lines.append(f'    (property "Value" "DAC1_OUT" (at 0 3 0) (layer "F.Fab") (effects (font (size 1 1))))')
    lines.append(f'    (uuid "{uid}")')
    lines.append(th_pad("1", 0, -1.27, drill=1.0, size=1.8))
    lines.append(th_pad("2", 0,  1.27, drill=1.0, size=1.8))
    lines.append('  )')
    lines.append('')

    # J3 DAC2_OUT connector
    uid = make_uuid("J3fp")
    lines.append(f'  (footprint "Connector_PinHeader_2.54mm:PinHeader_1x02_P2.54mm_Vertical"'
                 f' (layer "F.Cu") (at 115 60))')
    lines.append(f'    (property "Reference" "J3" (at 0 -3 0) (layer "F.Fab") (effects (font (size 1 1))))')
    lines.append(f'    (property "Value" "DAC2_OUT" (at 0 3 0) (layer "F.Fab") (effects (font (size 1 1))))')
    lines.append(f'    (uuid "{uid}")')
    lines.append(th_pad("1", 0, -1.27, drill=1.0, size=1.8))
    lines.append(th_pad("2", 0,  1.27, drill=1.0, size=1.8))
    lines.append('  )')
    lines.append('')

    # Silkscreen labels
    for txt, x, y in [
        ("J1 12V IN", j1x-8, j1y-6),
        ("U2 Buck 5V", u2x-8, u2y-7),
        ("U3 Boost 15V", u3x-8, u3y-7),
        ("U1 LoRa32 V2", u1x-14, u1y-14),
        ("U4 LM324", u4x-8, u4y-11),
        ("DAC1_OUT J2", 112, 37),
        ("DAC2_OUT J3", 112, 57),
        ("!GPIO25=LED on some revisions!", 55, 73),
    ]:
        uid = make_uuid(f"silk{txt}{x}{y}")
        lines.append(f'  (gr_text "{txt}" (at {x} {y} 0) (layer "F.SilkS") (uuid "{uid}")')
        lines.append(f'    (effects (font (size 0.8 0.8))))')
        lines.append('')

    lines.append(')')
    return "\n".join(lines)


def generate_project():
    return """{
  "board": {
    "3dviewports": [],
    "design_settings": {},
    "layer_presets": [],
    "viewports": []
  },
  "boards": [],
  "cvpcb": {
    "equivalence_files": []
  },
  "erc": {
    "erc_exclusions": [],
    "meta": { "version": 0 },
    "pin_map": [],
    "rules": []
  },
  "libraries": {
    "pinned_footprint_libs": [],
    "pinned_symbol_libs": []
  },
  "meta": {
    "filename": "fast_load_controller.kicad_pro",
    "version": 1
  },
  "net_settings": {
    "classes": [
      {
        "bus_width": 12,
        "clearance": 0.2,
        "diff_pair_gap": 0.25,
        "diff_pair_via_gap": 0.25,
        "diff_pair_width": 0.2,
        "line_style": 0,
        "microvia_diameter": 0.3,
        "microvia_drill": 0.1,
        "name": "Default",
        "pcb_color": "rgba(0, 0, 0, 0.000)",
        "schematic_color": "rgba(0, 0, 0, 0.000)",
        "track_width": 0.25,
        "via_diameter": 0.8,
        "via_drill": 0.4,
        "wire_width": 6,
        "bus_width": 12
      }
    ],
    "meta": { "version": 3 },
    "net_colors": null,
    "netclass_assignments": null,
    "netclass_patterns": []
  },
  "pcbnew": {
    "last_paths": {},
    "page_layout_descr_file": ""
  },
  "schematic": {
    "annotate_start_num": 0,
    "drawing": {
      "dashed_lines_dash_length_ratio": 12.0,
      "dashed_lines_gap_length_ratio": 3.0,
      "default_line_thickness": 6.0,
      "default_text_size": 50.0,
      "field_names": [],
      "intersheets_ref_own_page": false,
      "intersheets_ref_prefix": "",
      "intersheets_ref_short": false,
      "intersheets_ref_show": false,
      "intersheets_ref_suffix": "",
      "junction_size_choice": 3,
      "label_size_ratio": 0.375,
      "operating_point_overlay_i_precision": 3,
      "operating_point_overlay_i_range": "~A",
      "operating_point_overlay_v_precision": 3,
      "operating_point_overlay_v_range": "~V",
      "overbar_offset_ratio": 1.23,
      "pin_symbol_size": 25.0,
      "text_offset_ratio": 0.15
    },
    "legacy_lib_dir": "",
    "legacy_lib_list": [],
    "meta": { "version": 1 },
    "net_format_name": "",
    "ngspice": {
      "fix_include_paths": true,
      "fix_passive_vals": false,
      "workbook_filename": ""
    },
    "page_layout_descr_file": "",
    "plot_directory": "",
    "spice_adjust_passive_values": false,
    "spice_external_command": "spice \"%I\"",
    "subpart_first_id": 65,
    "subpart_id_separator": 0
  },
  "sheets": [
    ["fast_load_controller.kicad_sch", ""]
  ],
  "text_variables": {}
}
"""


if __name__ == "__main__":
    sch_path = os.path.join(OUT_DIR, "fast_load_controller.kicad_sch")
    pcb_path = os.path.join(OUT_DIR, "fast_load_controller.kicad_pcb")
    pro_path = os.path.join(OUT_DIR, "fast_load_controller.kicad_pro")

    print("Generating KiCad 8 project files...")

    sch_content = generate_schematic()
    with open(sch_path, "w") as f:
        f.write(sch_content)
    print(f"  Schematic : {sch_path}")

    pcb_content = generate_pcb()
    with open(pcb_path, "w") as f:
        f.write(pcb_content)
    print(f"  PCB       : {pcb_path}")

    pro_content = generate_project()
    with open(pro_path, "w") as f:
        f.write(pro_content)
    print(f"  Project   : {pro_path}")

    print("\nDone. Open fast_load_controller.kicad_pro in KiCad 8.")
    print("\nCircuit summary:")
    print("  J1  → 12V barrel jack input")
    print("  U2  → Buck module 12V→5V (powers U1 LoRa32)")
    print("  U3  → Boost module 12V→15V (powers U4 V+)")
    print("  U1  → Heltec LoRa32 V2 (ESP32 DAC GPIO25/GPIO26)")
    print("  U4  → LM324/LW354 quad op-amp (DIP-14)")
    print("  R1,R2 → 100Ω DAC protection series resistors")
    print("  R3,R4 → 3.3kΩ R_A (gain stage –input to GND)")
    print("  R5,R6 → 8.2kΩ R_B (gain stage feedback)")
    print("  J2  → DAC1_OUT 0–11.5V (GPIO25)")
    print("  J3  → DAC2_OUT 0–11.5V (GPIO26)")
    print("  Gain = 1 + 8.2k/3.3k = 3.485×")
    print("\n  ⚠  GPIO25 may conflict with onboard LED on some Heltec V2 PCB revisions.")
    print("     Use GPIO26 if DAC1 output shows LED loading.")
