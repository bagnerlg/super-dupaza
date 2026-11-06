import { readFileSync } from 'node:fs';
import { runInNewContext } from 'node:vm';
import test from 'node:test';
import assert from 'node:assert/strict';

const source = readFileSync(new URL('../src/index.js', import.meta.url), 'utf8');
const start = source.indexOf('const PENDING_COPIED_QTY_STORAGE =');
const end = source.indexOf('const PENDING_FIELDS_STORAGE =', start);
assert.ok(start > 0 && end > start, 'Encontrado módulo de copiado visual');
const snippet = source.slice(start, end);
function createContext() {
  const data = new Map();
  const events = [];
  const sessionStorage = {
    getItem(k) { return data.get(k) ?? null; },
    setItem(k, v) { data.set(k, v); }
  };
  const context = {
    sessionStorage,
    pendingHasMultipleUnits(quantity) { return Number(quantity) > 1; },
    renderNotLoaded() { events.push('rerender'); },
    visiblePendingColumns() { return [{key:'quantity',label:'Cantidad'}, {key:'customerName',label:'Cliente'}]; },
    async copyText(text, msg) { events.push({ text, msg }); return true; },
    copyToast(msg) { events.push(msg); },
    visibleOrderRows: [],
  };
  runInNewContext(snippet, context);
  return { context, events, data, run: (js) => runInNewContext(js, context) };
}

// Un combo madre y tres filas hijas, seguido de un código padre individual.
const example = [
  { trackingCode:'A', _lineKey:'PADRE', sku:'A2061A004', quantity:1, _firstOrderRow:true },
  { trackingCode:'A', _lineKey:'HIJO-1', sku:'B-2E-L-FR-0010', quantity:1, _firstOrderRow:false },
  { trackingCode:'A', _lineKey:'HIJO-2', sku:'P-CS-L-B-0008', quantity:2, _firstOrderRow:false },
  { trackingCode:'A', _lineKey:'HIJO-3', sku:'P-M-L-W-0001', quantity:2, _firstOrderRow:false },
  { trackingCode:'B', _lineKey:'PADRE', sku:'P-M-L-W-0001', quantity:1, _firstOrderRow:true },
];

test('copiar padres mantiene tres líneas vacías entre los dos valores 1', () => {
  const { context, run } = createContext();
  context.visibleOrderRows = example;
  assert.equal(run('pendingCopyValuesWithSpaces(visibleOrderRows, "quantity", true)'), '1\n\n\n\n1');
});

test('alerta se elimina tras copiar solo el SKU/cantidad correspondiente y no se modifican otras alertas', () => {
  const { run } = createContext();
  assert.equal(run('pendingNeedsUnitReview(' + JSON.stringify(example[2]) + ')'), true);
  run('pendingRememberCopiedQuantities([' + JSON.stringify(example[2]) + '])');
  assert.equal(run('pendingNeedsUnitReview(' + JSON.stringify(example[2]) + ')'), false);
  assert.equal(run('pendingNeedsUnitReview(' + JSON.stringify(example[3]) + ')'), true);
});

test('marcar otro padre en el mismo tracking crea un valor sin desalinear hijos', () => {
  const { run, context } = createContext();
  context.visibleOrderRows = example;
  run('pendingExtraParents.add(pendingUiLineKey(visibleOrderRows[3]))');
  assert.equal(run('pendingCopyValuesWithSpaces(visibleOrderRows, "quantity", true)'), '1\n\n\n2\n1');
});

test('cambio de cantidad reactiva alerta pendiente', () => {
  const { run } = createContext();
  const row = {...example[2], quantity:2};
  run('pendingRememberCopiedQuantities([' + JSON.stringify(row) + '])');
  assert.equal(run('pendingNeedsUnitReview(' + JSON.stringify({...row,quantity:3}) + ')'), true);
});

test('copiar columna normal conserva cantidades de TODOS los hijos, padres solo las principales', () => {
  const { context, run } = createContext();
  context.visibleOrderRows = example;
  assert.equal(run('pendingCopyValuesWithSpaces(visibleOrderRows, "quantity", false)'), '1\n1\n2\n2\n1');
  assert.equal(run('pendingCopyValuesWithSpaces(visibleOrderRows, "quantity", true)'), '1\n\n\n\n1');
});
