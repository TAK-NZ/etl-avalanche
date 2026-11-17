import test from 'node:test';
import assert from 'node:assert';
import { SchemaType, DataFlowType } from '@tak-ps/etl';

// task.ts calls Task.init() at module scope which requires an ETL environment,
// so these must be set before the dynamic import below
process.env.ETL_API = process.env.ETL_API || 'http://localhost:5001';
process.env.ETL_LAYER = process.env.ETL_LAYER || '1';
process.env.ETL_TOKEN = process.env.ETL_TOKEN || 'etl.test-token';

const { default: Task } = await import('../task.js');

test('Task static config', () => {
    assert.equal(Task.name, 'etl-avalanche');
    assert.deepEqual(Task.flow, [DataFlowType.Incoming]);
});

test('Incoming Input schema', async () => {
    const task = await Task.init();
    const schema = await task.schema(SchemaType.Input, DataFlowType.Incoming);

    assert.equal(schema.type, 'object');
    for (const key of [
        'Timeout'
    ]) {
        assert.ok(schema.properties[key], `Env schema missing property: ${key}`);
    }
});

test('Incoming Output schema', async () => {
    const task = await Task.init();
    const schema = await task.schema(SchemaType.Output, DataFlowType.Incoming);

    assert.equal(schema.type, 'object');
    for (const key of [
        'dangerLevel',
        'dangerLevelText',
        'region',
        'regionId',
        'description',
        'issuedUTC',
        'issuedLocal',
        'expired'
    ]) {
        assert.ok(schema.properties[key], `Output schema missing property: ${key}`);
    }
});

const { buildRegionFeatures } = await import('../task.js');
const { CoTParser } = await import('@tak-ps/node-cot');

const RING = [[170, -43, 0], [171, -43, 0], [171, -44, 0], [170, -44, 0], [170, -43, 0]];
const STALE = '2030-01-02T03:04:05.000Z';

const baseProperties = {
    callsign: 'Avalanche Risk: Test - Moderate (2)',
    type: 'a-f-X-i-g-a',
    time: '2030-01-01T00:00:00.000Z',
    start: '2030-01-01T00:00:00.000Z',
    stale: STALE,
    links: [{
        uid: 'avalanche-7',
        relation: 'r-u',
        mime: 'text/html',
        url: 'https://www.avalanche.net.nz/region/7',
        remarks: 'Avalanche Forecast Details'
    }]
};

function build(polygon: boolean) {
    return buildRegionFeatures({
        regionId: 7,
        level: 2,
        color: 'rgb(255, 244, 31)',
        url: 'https://www.avalanche.net.nz/region/7',
        polygonCoordinates: polygon ? [RING] : null,
        centerCoords: [170.5, -43.5],
        baseProperties
    });
}

test('Region is a polygon marker plus a separate icon point', () => {
    const [polygon, point, ...rest] = build(true);
    assert.deepEqual(rest, []);

    assert.equal(polygon.id, 'avalanche-7');
    assert.equal(polygon.geometry.type, 'Polygon');
    assert.equal(polygon.properties.type, 'a-f-X-i-g-a');
    assert.equal(polygon.properties.icon, undefined, 'a usericon would make ATAK delete the polygon');
    assert.equal(polygon.properties.center, undefined);
    assert.equal(polygon.properties['marker-opacity'], 0);

    assert.equal(point.id, 'avalanche-7-center');
    assert.deepEqual(point.geometry, { type: 'Point', coordinates: [170.5, -43.5] });
    assert.ok(String(point.properties.icon).includes('DangerLevel2'));
    assert.equal(point.properties['marker-opacity'], undefined);
    assert.equal(point.properties.stale, polygon.properties.stale);

    for (const feat of [polygon, point]) {
        assert.strictEqual(feat.properties.marti_archive, false);
    }
});

test('Region without a polygon is only the icon point', () => {
    const feats = build(false);
    assert.equal(feats.length, 1);
    assert.equal(feats[0].id, 'avalanche-7-center');
    assert.equal(feats[0].geometry.type, 'Point');
});

test('Polygon marker converts to an invisible, expiring marker with a polyline', async () => {
    const [polygon] = build(true);
    const event = (await CoTParser.from_geojson(polygon as never)).raw.event;

    // A u-d-f drawing would never expire in ATAK
    assert.equal(event._attributes.type, 'a-f-X-i-g-a');
    assert.equal(event._attributes.stale, STALE);
    assert.equal(event.detail.usericon, undefined, 'a usericon would make ATAK delete the polygon');
    assert.strictEqual(event.detail.marti?._attributes?.archive, false);

    // Alpha 0 hides the marker's unknown-type "?" icon in ATAK
    const argb = Number(event.detail.color?._attributes?.argb);
    assert.ok(Number.isFinite(argb));
    assert.equal((argb >>> 24) & 0xff, 0);

    const polyline = event.detail.shape?.polyline;
    assert.ok(polyline, 'detail.shape.polyline is set');
    assert.equal(polyline._attributes?.closed, true);
    assert.equal(polyline.vertex.length, RING.length);
    for (const vertex of polyline.vertex) {
        assert.equal(vertex._attributes.hae, undefined, 'vertices carry no altitude');
    }
});

test('Icon point converts to a plain marker with the icon and no shape', async () => {
    const [, point] = build(true);
    const event = (await CoTParser.from_geojson(point as never)).raw.event;

    assert.equal(event._attributes.stale, STALE);
    assert.equal(event.point._attributes.lon, 170.5);
    assert.equal(event.point._attributes.lat, -43.5);
    assert.ok(event.detail.usericon, 'icon is carried on the point');
    assert.equal(event.detail.shape, undefined);
    assert.strictEqual(event.detail.marti?._attributes?.archive, false);
});
