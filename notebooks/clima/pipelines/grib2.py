# Minimal GRIB2 decoder in numpy (no eccodes).
# Supports grid 3.0 (regular lat/lon), packing 5.0 (simple), 5.2 (complex) and 5.3 (complex + spatial differencing),
# and the section-6 bitmap. Anything else fails with a clear message. Bit unpacking is vectorized: every value's bit
# offset and width are computed up front, and the bits are gathered as one (n_values x max_width) matrix.

from typing import List, NamedTuple

import numpy as np


class Grid(NamedTuple):
    values: np.ndarray  # float64, nlat*nlon, row-major (row = latitude)
    nlat: int
    nlon: int
    lat0: float
    dlat: float
    lon0: float
    dlon: float


class Msg(NamedTuple):
    cat: int
    num: int
    tpl_data: int
    grid: Grid


def _u(b: bytes, o: int, n: int) -> int:
    return int.from_bytes(b[o:o + n], "big")


def _s(b: bytes, o: int, n: int) -> int:
    """GRIB2 signed integer: top bit = sign, rest = magnitude"""
    v = _u(b, o, n)
    top = 1 << (8 * n - 1)
    return -(v - top) if v >= top else v


def _f32(b: bytes, o: int) -> float:
    return float(np.frombuffer(b[o:o + 4], dtype=">f4")[0])


class _Bits:
    def __init__(self, buf: bytes, byte_pos: int, byte_end: int):
        self.bits = np.unpackbits(np.frombuffer(buf, dtype=np.uint8, count=byte_end - byte_pos, offset=byte_pos))
        self.pos = 0

    def read(self, n: int) -> int:
        v = 0
        for k in self.bits[self.pos:self.pos + n]:
            v = (v << 1) | int(k)
        self.pos += n
        return v

    def read_signed(self, nbytes: int) -> int:
        v = self.read(8 * nbytes)
        top = 1 << (8 * nbytes - 1)
        return -(v - top) if v >= top else v

    def fixed(self, count: int, width: int) -> np.ndarray:
        """count consecutive unsigned integers of `width` bits"""
        if width == 0 or count == 0:
            return np.zeros(count, dtype=np.int64)
        m = self.bits[self.pos:self.pos + count * width].reshape(count, width).astype(np.int64)
        self.pos += count * width
        return m.dot(np.int64(1) << np.arange(width - 1, -1, -1, dtype=np.int64))

    def variable(self, widths: np.ndarray) -> np.ndarray:
        """one unsigned integer per entry of `widths` (bit widths), packed back to back"""
        n = len(widths)
        out = np.zeros(n, dtype=np.int64)
        if n == 0:
            return out
        offs = self.pos + np.concatenate(([0], np.cumsum(widths[:-1])))
        self.pos += int(widths.sum())
        maxw = int(widths.max())
        if maxw == 0:
            return out
        idx = np.minimum(offs[:, None] + np.arange(maxw)[None, :], len(self.bits) - 1)
        m = self.bits[idx].astype(np.int64)
        raw = m.dot(np.int64(1) << np.arange(maxw - 1, -1, -1, dtype=np.int64))
        return np.where(widths > 0, raw >> (maxw - widths), 0)

    def align(self):
        self.pos = -(-self.pos // 8) * 8


def _unpack(buf: bytes, start: int, end: int, d: dict) -> np.ndarray:
    n = d["npk"]
    bs, ds = 2.0 ** d["E"], 10.0 ** -d["D"]
    if d["tpl"] == 0:
        if d["nb"] == 0:
            return np.full(n, d["R"] * ds)
        return (d["R"] + _Bits(buf, start, end).fixed(n, d["nb"]) * bs) * ds
    if d["tpl"] not in (2, 3):
        raise ValueError("empacotamento 5.%d não suportado (só 5.0, 5.2, 5.3)" % d["tpl"])
    br = _Bits(buf, start, end)
    order = d.get("order", 0) if d["tpl"] == 3 else 0
    ival1 = ival2 = minsd = 0
    if order > 0:
        ival1 = br.read_signed(d["ospd"])
        if order == 2:
            ival2 = br.read_signed(d["ospd"])
        minsd = br.read_signed(d["ospd"])
    ng = d["ng"]
    gref = br.fixed(ng, d["nb"])
    br.align()
    gw = br.fixed(ng, d["nbw"]) + d["refw"]
    br.align()
    gl = d["refl"] + br.fixed(ng, d["nbl"]) * d["incl"]
    br.align()
    if ng > 0:
        gl[-1] = d["lastl"]
    tot = int(gl.sum())
    if tot < n:
        raise ValueError("GRIB2 com %d valores nos grupos, esperados %d" % (tot, n))
    w = np.repeat(gw, gl)[:n]
    r = np.repeat(gref, gl)[:n]
    v = br.variable(w)
    x = r + v
    miss = np.zeros(n, dtype=bool)
    mm, nb = d["miss"], d["nb"]
    if mm >= 1:
        zero_w = w == 0
        miss |= zero_w & (nb > 0) & (r == (1 << nb) - 1)
        miss |= ~zero_w & (v == (np.int64(1) << w) - 1)
        if mm == 2:
            miss |= zero_w & (nb > 0) & (r == (1 << nb) - 2)
            miss |= ~zero_w & (v == (np.int64(1) << w) - 2)
    if order > 0:  # undo the spatial differencing over the non-missing values only
        ok = np.flatnonzero(~miss)
        dv = x[ok].astype(np.float64)
        y = np.empty_like(dv)
        if len(dv):
            y[0] = ival1
            if order == 1:
                y[1:] = ival1 + np.cumsum(dv[1:] + minsd)
            elif len(dv) > 1:
                y[1] = ival2
                z = (ival2 - ival1) + np.cumsum(dv[2:] + minsd)  # first differences from index 2 on
                y[2:] = ival2 + np.cumsum(z)
        xf = x.astype(np.float64)
        xf[ok] = y
    else:
        xf = x.astype(np.float64)
    out = (d["R"] + xf * bs) * ds
    out[miss] = np.nan
    return out


def decode(buf: bytes) -> List[Msg]:
    msgs: List[Msg] = []
    mpos = 0
    while mpos + 16 <= len(buf):
        if buf[mpos:mpos + 4] != b"GRIB":
            break
        if buf[mpos + 7] != 2:
            raise ValueError("GRIB edição %d não suportada" % buf[mpos + 7])
        total = _u(buf, mpos + 8, 8)
        pos = mpos + 16
        grid = prod = drs = None
        bitmap = None  # byte offset of the bitmap, or None
        while pos < mpos + total - 4:
            if buf[pos:pos + 4] == b"7777":
                break
            ln, sec, o = _u(buf, pos, 4), buf[pos + 4], pos  # octet k (1-based) of the section is at o + k - 1
            if sec == 3:
                tpl = _u(buf, o + 12, 2)
                if tpl != 0:
                    raise ValueError("grade 3.%d não suportada (só 3.0 lat/lon regular)" % tpl)
                grid = {"ni": _u(buf, o + 30, 4), "nj": _u(buf, o + 34, 4), "la1": _s(buf, o + 46, 4) / 1e6,
                        "lo1": _s(buf, o + 50, 4) / 1e6, "di": _u(buf, o + 63, 4) / 1e6, "dj": _u(buf, o + 67, 4) / 1e6,
                        "scan": buf[o + 71]}
            elif sec == 4:
                prod = {"tpl": _u(buf, o + 7, 2), "cat": buf[o + 9], "num": buf[o + 10]}
            elif sec == 5:
                tpl = _u(buf, o + 9, 2)
                drs = {"npk": _u(buf, o + 5, 4), "tpl": tpl, "R": _f32(buf, o + 11), "E": _s(buf, o + 15, 2),
                       "D": _s(buf, o + 17, 2), "nb": buf[o + 19]}
                if tpl in (2, 3):
                    drs.update(miss=buf[o + 22], ng=_u(buf, o + 31, 4), refw=buf[o + 35], nbw=buf[o + 36], refl=_u(buf, o + 37, 4),
                               incl=buf[o + 41], lastl=_u(buf, o + 42, 4), nbl=buf[o + 46])
                    if tpl == 3:
                        drs.update(order=buf[o + 47], ospd=buf[o + 48])
            elif sec == 6:
                ind = buf[o + 5]
                if ind == 0:
                    bitmap = o + 6
                elif ind == 255:
                    bitmap = None
                else:
                    raise ValueError("bitmap predefinido (%d) não suportado" % ind)
            elif sec == 7:
                if not grid or not drs:
                    raise ValueError("GRIB2 sem seção 3 ou 5 antes dos dados")
                if grid["scan"] & 0x30:
                    raise ValueError("scanning mode %d não suportado" % grid["scan"])
                packed = _unpack(buf, o + 5, o + ln, drs)
                npts = grid["ni"] * grid["nj"]
                if bitmap is not None:
                    mask = np.unpackbits(np.frombuffer(buf, dtype=np.uint8, count=-(-npts // 8), offset=bitmap))[:npts].astype(bool)
                    values = np.full(npts, np.nan)
                    values[mask] = packed[:int(mask.sum())]
                else:
                    values = np.full(npts, np.nan)
                    k = min(npts, len(packed))
                    values[:k] = packed[:k]
                isg = -1 if grid["scan"] & 0x80 else 1  # 0 = west→east
                jsg = 1 if grid["scan"] & 0x40 else -1  # 0 = north→south
                lon0 = grid["lo1"] - 360 if grid["lo1"] > 180 else grid["lo1"]
                msgs.append(Msg(prod["cat"] if prod else -1, prod["num"] if prod else -1, drs["tpl"],
                                Grid(values, grid["nj"], grid["ni"], grid["la1"], jsg * grid["dj"], lon0, isg * grid["di"])))
            pos += ln
        mpos += total
    return msgs


def pick_precip(msgs: List[Msg]) -> Msg:
    """precipitation message (category 1 = moisture); falls back to the first message (MERGE stores rain as category 15/5)"""
    if not msgs:
        raise ValueError("GRIB2 sem mensagens")
    for m in msgs:
        if m.cat == 1 and m.num in (8, 52, 9, 10):
            return m
    for m in msgs:
        if m.cat == 1:
            return m
    return msgs[0]
