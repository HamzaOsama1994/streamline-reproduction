// Markdown subset -> Word document laid out like an Elsevier (Future Generation Computer Systems) article.
// Usage: node fgcs.js REPORT.md OUT.docx   (figures are read relative to REPORT.md)
const fs = require("fs");
const path = require("path");
const {
  Document, Packer, Paragraph, TextRun, Table, TableRow, TableCell, WidthType, ShadingType, AlignmentType,
  LevelFormat, BorderStyle, ImageRun, SectionType, Footer, Header, PageNumber, TabStopType, VerticalAlign,
} = require("docx");

const [, , SRC, OUT] = process.argv;
const BASE = path.dirname(SRC);
const md = fs.readFileSync(SRC, "utf8").replace(/\r/g, "");

const FONT = "Times New Roman";
const SZ = 19;            // 9.5 pt body
const SMALL = 16;         // 8 pt tables, captions, references
const PAGE_W = 11906, PAGE_H = 16838, MX = 850, MT = 1000;
const FULL = PAGE_W - 2 * MX;          // 10206 dxa
const GAP = 340;
const COL = Math.floor((FULL - GAP) / 2);  // 4933 dxa
const RULE = { style: BorderStyle.SINGLE, size: 4, color: "000000" };
const NONE = { style: BorderStyle.NONE, size: 0, color: "FFFFFF" };

// ---------- inline formatting ----------
function inline(text, base = {}) {
  const out = [];
  const re = /(\[\[[^\]]*\]\]|\*\*[^*]+\*\*|\*[^*]+\*|`[^`]+`)/g;
  let last = 0, m;
  const push = (t, o) => { if (t) out.push(...subscripts(t, { ...base, ...o })); };
  while ((m = re.exec(text))) {
    push(text.slice(last, m.index), {});
    const tok = m[0];
    if (tok.startsWith("[[")) push(tok, { highlight: "yellow" });
    else if (tok.startsWith("**")) out.push(...inline(tok.slice(2, -2), { ...base, bold: true }));
    else if (tok.startsWith("`")) out.push(new TextRun({ text: tok.slice(1, -1), font: "Courier New", size: (base.size || SZ) - 2, bold: base.bold, italics: base.italics }));
    else out.push(...inline(tok.slice(1, -1), { ...base, italics: true }));
    last = m.index + tok.length;
  }
  push(text.slice(last), {});
  return out;
}
// v_0 / x_i / s_i -> subscript; a^b -> superscript (used in author lines)
function subscripts(t, o) {
  const runs = [];
  const re = /\b([A-Za-z])_([0-9a-z]{1,2})\b|\^([a-z*,]+)/g;
  let last = 0, m;
  const R = (text, extra = {}) => new TextRun({ text, font: o.font || FONT, size: o.size || SZ, bold: o.bold, italics: o.italics, highlight: o.highlight, ...extra });
  while ((m = re.exec(t))) {
    if (m.index > last) runs.push(R(t.slice(last, m.index)));
    if (m[1]) { runs.push(R(m[1])); runs.push(R(m[2], { subScript: true })); }
    else runs.push(R(m[3], { superScript: true }));
    last = m.index + m[0].length;
  }
  if (last < t.length) runs.push(R(t.slice(last)));
  return runs;
}

// ---------- block parsing ----------
const front = {}, highlights = [];
let body = md;
body = body.replace(/::: front\n([\s\S]*?)\n:::/, (_, s) => {
  s.split("\n").forEach((l) => { const i = l.indexOf(":"); if (i > 0) front[l.slice(0, i).trim()] = l.slice(i + 1).trim(); });
  return "";
});
body = body.replace(/::: highlights\n([\s\S]*?)\n:::/, (_, s) => {
  s.split("\n").forEach((l) => { if (l.startsWith("- ")) highlights.push(l.slice(2)); });
  return "";
});

const lines = body.split("\n");
const blocks = [];
for (let i = 0; i < lines.length;) {
  const l = lines[i];
  if (!l.trim()) { i++; continue; }
  if (l.startsWith("## ")) { blocks.push({ t: "h2", text: l.slice(3) }); i++; continue; }
  if (l.startsWith("# ")) { blocks.push({ t: "h1", text: l.slice(2) }); i++; continue; }
  if (l.startsWith("Table: ")) { blocks.push({ t: "tcap", text: l.slice(7) }); i++; continue; }
  if (l.startsWith("![")) {
    const m = l.match(/^!\[(.*)\]\((.*?)\)(\{full\})?/);
    blocks.push({ t: "img", cap: m[1], file: m[2], full: !!m[3] }); i++; continue;
  }
  if (l.startsWith("|")) {
    const rows = [];
    while (i < lines.length && lines[i].startsWith("|")) {
      if (!/^\|[-| :]+\|$/.test(lines[i])) rows.push(lines[i].slice(1, -1).split("|").map((c) => c.trim()));
      i++;
    }
    blocks.push({ t: "table", rows }); continue;
  }
  if (/^- /.test(l)) {
    const items = [];
    while (i < lines.length && /^- /.test(lines[i])) { items.push(lines[i].slice(2)); i++; }
    blocks.push({ t: "ul", items }); continue;
  }
  if (/^\d+\. /.test(l)) {
    const items = [];
    while (i < lines.length && /^\d+\. /.test(lines[i])) { items.push(lines[i].replace(/^\d+\. /, "")); i++; }
    blocks.push({ t: "ol", items }); continue;
  }
  const para = [];
  while (i < lines.length && lines[i].trim() && !/^(#|\||- |\d+\. |!\[|Table: )/.test(lines[i])) { para.push(lines[i]); i++; }
  blocks.push({ t: "p", text: para.join(" ") });
}

// ---------- element builders ----------
const P = (children, o = {}) => new Paragraph({ children, alignment: AlignmentType.JUSTIFIED, spacing: { after: 0, line: 228 }, indent: o.noIndent ? undefined : { firstLine: 240 }, ...o.p });
let olCount = 0;
function list(items, ordered) {
  const ref = ordered ? "ol" : "ul";
  const inst = ordered ? ++olCount : 0;
  return items.map((it) => new Paragraph({ children: inline(it), alignment: AlignmentType.JUSTIFIED, numbering: { reference: ref, level: 0, instance: inst },
    spacing: { after: 20, line: 228 } }));
}
function table(rows, width) {
  const n = rows[0].length;
  // Auto layout (as browsers do): every column gets at least its longest word, and the remaining width is shared in
  // proportion to how much longer the column's longest line is. Lines inside a cell are separated by <br>.
  const CH = 88; // dxa per character at 8 pt (approximate)
  const PAD = 140;
  const linesOf = (c) => (c || "").replace(/\*\*/g, "").split("<br>");
  const nat = rows[0].map((_, j) => Math.max(...rows.map((r) => Math.max(...linesOf(r[j]).map((l) => l.length)))) * CH + PAD);
  const min = rows[0].map((_, j) => Math.max(...rows.map((r) => Math.max(...linesOf(r[j]).flatMap((l) => l.split(/[ 	]+/)).map((w) => w.length)))) * CH + PAD);
  let widths;
  const natTot = nat.reduce((a, b) => a + b, 0), minTot = min.reduce((a, b) => a + b, 0);
  if (natTot <= width) {
    widths = nat.map((x) => Math.floor(x + (width - natTot) * (x / natTot)));
  } else {
    const slack = nat.map((x, j) => x - min[j]);
    const sTot = slack.reduce((a, b) => a + b, 0) || 1;
    const room = Math.max(0, width - minTot);
    widths = min.map((x, j) => Math.floor(x + (room * slack[j]) / sTot));
    if (minTot > width) widths = min.map((x) => Math.floor((width * x) / minTot));
  }
  widths[n - 1] += width - widths.reduce((a, b) => a + b, 0);
  const last = rows.length - 1;
  return new Table({
    width: { size: width, type: WidthType.DXA }, columnWidths: widths,
    borders: { top: RULE, bottom: RULE, left: NONE, right: NONE, insideHorizontal: NONE, insideVertical: NONE },
    rows: rows.map((r, i) => new TableRow({
      tableHeader: i === 0, cantSplit: true,
      children: r.map((c, j) => new TableCell({
        width: { size: widths[j], type: WidthType.DXA }, verticalAlign: VerticalAlign.TOP,
        shading: i === 0 ? { fill: "EEF2F7", type: ShadingType.CLEAR, color: "auto" } : undefined,
        borders: { top: i === 1 ? RULE : NONE, bottom: i === 0 ? RULE : (i < last ? { style: BorderStyle.SINGLE, size: 2, color: "D0D5DD" } : NONE), left: NONE, right: NONE },
        margins: { top: 45, bottom: 45, left: 70, right: 70 },
        children: (c || "").split("<br>").map((part) => new Paragraph({ keepNext: i < last, spacing: { after: 0, line: 216 },
          children: inline(part, { size: SMALL, bold: i === 0 ? true : undefined }) })),
      })),
    })),
  });
}
function caption(text, above) {
  const m = text.match(/^((?:Table|Fig\.) [A-Z0-9.]+\.?)(.*)$/);
  const head = m ? m[1] : "", rest = m ? m[2] : text;
  return new Paragraph({ spacing: { before: above ? 120 : 60, after: above ? 60 : 160, line: 216 }, keepNext: above,
    children: [new TextRun({ text: head, font: FONT, size: SMALL, bold: true }), ...inline(rest, { size: SMALL })] });
}
function image(file, width) {
  if (!fs.existsSync(path.join(BASE, file)))
    return new Paragraph({ alignment: AlignmentType.CENTER, spacing: { before: 120 }, keepNext: true, children: inline(`[[figure ${file} not generated yet]]`) });
  const buf = fs.readFileSync(path.join(BASE, file));
  const w = buf.readUInt32BE(16), h = buf.readUInt32BE(20); // PNG IHDR
  const px = Math.round((width / 1440) * 96);
  return new Paragraph({ alignment: AlignmentType.CENTER, spacing: { before: 120, after: 0 }, keepNext: true,
    children: [new ImageRun({ type: "png", data: buf, transformation: { width: px, height: Math.round((px * h) / w) } })] });
}

// ---------- front matter ----------
const fm = [];
fm.push(new Paragraph({ spacing: { after: 60 }, border: { bottom: { style: BorderStyle.SINGLE, size: 6, color: "000000", space: 2 } },
  children: [new TextRun({ text: "AI508 reproduction report · prepared in Future Generation Computer Systems (Elsevier) article format", font: FONT, size: SMALL, italics: true })] }));
fm.push(new Paragraph({ spacing: { before: 240, after: 160, line: 300 }, children: [new TextRun({ text: front.title, font: FONT, size: 34 })] }));
fm.push(new Paragraph({ spacing: { after: 100 }, children: front.authors.split(";").flatMap((a, k, arr) => [...inline(a.trim(), { size: 21 }), ...(k < arr.length - 1 ? [new TextRun({ text: ", ", font: FONT, size: 21 })] : [])]) }));
const [aKey, aText] = front.affiliation.split("|").map((s) => s.trim());
fm.push(new Paragraph({ spacing: { after: 40 }, children: [new TextRun({ text: aKey, font: FONT, size: SMALL, superScript: true, italics: true }), ...inline(" " + aText, { size: SMALL, italics: true })] }));
fm.push(new Paragraph({ spacing: { after: 40 }, children: inline(front.corresponding, { size: SMALL }) }));
fm.push(new Paragraph({ spacing: { after: 160 }, children: inline(front.note, { size: SMALL, italics: true }) }));
// highlights box
fm.push(new Table({
  width: { size: FULL, type: WidthType.DXA }, columnWidths: [FULL],
  rows: [new TableRow({ children: [new TableCell({
    width: { size: FULL, type: WidthType.DXA }, shading: { fill: "EEF2F7", type: ShadingType.CLEAR, color: "auto" },
    borders: { top: NONE, bottom: NONE, left: NONE, right: NONE }, margins: { top: 100, bottom: 100, left: 160, right: 160 },
    children: [new Paragraph({ spacing: { after: 60 }, children: [new TextRun({ text: "H I G H L I G H T S", font: FONT, size: SMALL, bold: true })] }),
      ...highlights.map((h) => new Paragraph({ numbering: { reference: "ul", level: 0, instance: 0 }, spacing: { after: 20 }, children: inline(h, { size: SMALL + 1 }) }))],
  })] })],
}));
// article info | abstract
const absIdx = blocks.findIndex((b) => b.t === "h1" && b.text === "Abstract");
const abstract = blocks[absIdx + 1].text;
blocks.splice(absIdx, 2);
const L = 2900, Rw = FULL - L - 200;
fm.push(new Paragraph({ spacing: { after: 0 }, children: [] }));
fm.push(new Table({
  width: { size: FULL, type: WidthType.DXA }, columnWidths: [L, 200, Rw],
  borders: { top: NONE, bottom: NONE, left: NONE, right: NONE, insideHorizontal: NONE, insideVertical: NONE },
  rows: [new TableRow({ children: [
    new TableCell({ width: { size: L, type: WidthType.DXA }, borders: { top: RULE, bottom: RULE, left: NONE, right: NONE }, margins: { top: 80, bottom: 80, left: 0, right: 60 },
      children: [new Paragraph({ spacing: { after: 80 }, border: { bottom: { style: BorderStyle.SINGLE, size: 2, color: "000000", space: 2 } }, children: [new TextRun({ text: "A R T I C L E   I N F O", font: FONT, size: SMALL, bold: true })] }),
        new Paragraph({ spacing: { after: 40 }, children: [new TextRun({ text: "Keywords:", font: FONT, size: SMALL, italics: true })] }),
        ...front.keywords.split(";").map((k) => new Paragraph({ spacing: { after: 0 }, children: [new TextRun({ text: k.trim(), font: FONT, size: SMALL })] }))] }),
    new TableCell({ width: { size: 200, type: WidthType.DXA }, borders: { top: NONE, bottom: NONE, left: NONE, right: NONE }, children: [new Paragraph("")] }),
    new TableCell({ width: { size: Rw, type: WidthType.DXA }, borders: { top: RULE, bottom: RULE, left: NONE, right: NONE }, margins: { top: 80, bottom: 80, left: 0, right: 0 },
      children: [new Paragraph({ spacing: { after: 80 }, border: { bottom: { style: BorderStyle.SINGLE, size: 2, color: "000000", space: 2 } }, children: [new TextRun({ text: "A B S T R A C T", font: FONT, size: SMALL, bold: true })] }),
        new Paragraph({ alignment: AlignmentType.JUSTIFIED, spacing: { after: 0, line: 228 }, children: inline(abstract, { size: SMALL + 1 }) })] }),
  ] })],
}));
fm.push(new Paragraph({ spacing: { after: 200 }, children: [] }));

// ---------- body: two columns, full-width sections for wide tables/figures ----------
const sections = [];
const pageProps = { page: { size: { width: PAGE_W, height: PAGE_H }, margin: { top: MT, bottom: MT, left: MX, right: MX, header: 500, footer: 500 } } };
const header = new Header({ children: [new Paragraph({ tabStops: [{ type: TabStopType.RIGHT, position: FULL }], border: { bottom: { style: BorderStyle.SINGLE, size: 2, color: "000000", space: 2 } },
  children: [new TextRun({ text: "Reproducing STREAMLINE", font: FONT, size: SMALL, italics: true }), new TextRun({ text: "\tAI508 Assignment 2", font: FONT, size: SMALL, italics: true })] })] });
const footer = new Footer({ children: [new Paragraph({ alignment: AlignmentType.CENTER, children: [new TextRun({ children: [PageNumber.CURRENT], font: FONT, size: SMALL })] })] });
sections.push({ properties: { ...pageProps }, headers: { default: header }, footers: { default: footer }, children: fm });
let cur = [];
const flush2 = () => { if (cur.length) sections.push({ properties: { ...pageProps, type: SectionType.CONTINUOUS, column: { count: 2, space: GAP, equalWidth: true } }, children: cur }); cur = []; };
const full = (children) => { flush2(); sections.push({ properties: { ...pageProps, type: SectionType.CONTINUOUS, column: { count: 1 } }, children }); };

let pendingCap = null, inRefs = false, afterHeading = false;
// a heading directly followed by a full-width table or figure travels with it (no orphaned heading above a section break)
const wideNext = (k) => (blocks[k] && blocks[k].t === "tcap" && blocks[k + 1] && blocks[k + 1].t === "table" && blocks[k + 1].rows[0].length >= 3)
  || (blocks[k] && blocks[k].t === "img" && blocks[k].full);
let heldHeading = [];
for (let bi = 0; bi < blocks.length; bi++) {
  const b = blocks[bi];
  if (b.t === "h1" || b.t === "h2") {
    if (b.t === "h1") inRefs = b.text === "References";
    const hp = b.t === "h1"
      ? new Paragraph({ spacing: { before: 200, after: 80 }, keepNext: true, children: inline(b.text, { bold: true }) })
      : new Paragraph({ spacing: { before: 120, after: 60 }, keepNext: true, children: inline(b.text, { italics: true }) });
    if (wideNext(bi + 1)) heldHeading.push(hp); else cur.push(hp);
    afterHeading = true; continue;
  }
  if (b.t === "tcap") { pendingCap = b.text; continue; }
  if (b.t === "table") {
    const wide = b.rows[0].length >= 3;
    const els = [caption(pendingCap || "", true), table(b.rows, wide ? FULL : COL), new Paragraph({ spacing: { after: 120 }, children: [] })];
    pendingCap = null;
    if (wide) { full([...heldHeading, ...els]); heldHeading = []; } else cur.push(...els);
    continue;
  }
  if (b.t === "img") {
    const els = [image(b.file, b.full ? Math.round(FULL * 0.92) : COL), caption(b.cap, false)];
    if (b.full) { full([...heldHeading, ...els]); heldHeading = []; } else cur.push(...els);
    continue;
  }
  if (b.t === "ul" || b.t === "ol") { cur.push(...list(b.items, b.t === "ol")); afterHeading = false; continue; }
  if (b.t === "p") {
    if (inRefs) {
      const m = b.text.match(/^(\[\d+\])\s*(.*)$/);
      cur.push(new Paragraph({ spacing: { after: 30, line: 216 }, indent: { left: 360, hanging: 360 }, tabStops: [{ type: TabStopType.LEFT, position: 360 }],
        children: [new TextRun({ text: m[1] + "\t", font: FONT, size: SMALL }), ...inline(m[2], { size: SMALL })] }));
    } else {
      cur.push(P(inline(b.text), { noIndent: afterHeading }));
    }
    afterHeading = false;
  }
}
flush2();

const doc = new Document({
  creator: "AI508 team", title: front.title,
  styles: { default: { document: { run: { font: FONT, size: SZ } } } },
  numbering: { config: [
    { reference: "ul", levels: [{ level: 0, format: LevelFormat.BULLET, text: "•", alignment: AlignmentType.LEFT, style: { paragraph: { indent: { left: 300, hanging: 200 } } } }] },
    { reference: "ol", levels: [{ level: 0, format: LevelFormat.DECIMAL, text: "(%1)", alignment: AlignmentType.LEFT, style: { paragraph: { indent: { left: 360, hanging: 300 } } } }] },
  ] },
  sections,
});
Packer.toBuffer(doc).then((buf) => { fs.writeFileSync(OUT, buf); console.log("written", OUT, sections.length, "sections"); });
