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

const { buildRegionFeature } = await import('../task.js');
const { CoTParser } = await import('@tak-ps/node-cot');

const RING = [[170, -43], [171, -43], [171, -44], [170, -44], [170, -43]];
const STALE = '2030-01-02T03:04:05.000Z';

const baseProperties = {
    callsign: 'Avalanche Risk: Test - Moderate (2)',
    type: 'a-f-X-i-g-a',
    time: '2030-01-01T00:00:00.000Z',
    start: '2030-01-01T00:00:00.000Z',
    stale: STALE
};

test('Region feature is a single Polygon carrying center and icon', () => {
    const feat = buildRegionFeature({
        regionId: 7,
        level: 2,
        color: 'rgb(255, 244, 31)',
        polygonCoordinates: [RING],
        centerCoords: [170.5, -43.5],
        baseProperties
    });

    assert.equal(feat.id, 'avalanche-7');
    assert.equal(feat.geometry.type, 'Polygon');
    assert.deepEqual(feat.properties.center, [170.5, -43.5]);
    assert.ok(String(feat.properties.icon).includes('DangerLevel2'));
    assert.equal(feat.properties.type, 'a-f-X-i-g-a');
    assert.strictEqual(feat.properties.marti_archive, false);
});

test('Region feature without a polygon falls back to a Point with the same id', () => {
    const feat = buildRegionFeature({
        regionId: 7,
        level: 2,
        color: 'rgb(255, 244, 31)',
        polygonCoordinates: null,
        centerCoords: [170.5, -43.5],
        baseProperties
    });

    assert.equal(feat.id, 'avalanche-7');
    assert.deepEqual(feat.geometry, { type: 'Point', coordinates: [170.5, -43.5] });
    assert.strictEqual(feat.properties.marti_archive, false);
});

test('Region feature converts to an expiring marker with a polyline shape', async () => {
    const feat = buildRegionFeature({
        regionId: 7,
        level: 2,
        color: 'rgb(255, 244, 31)',
        polygonCoordinates: [RING],
        centerCoords: [170.5, -43.5],
        baseProperties
    });

    const cot = await CoTParser.from_geojson(feat as never);
    const event = cot.raw.event;

    // A u-d-f drawing would never expire in ATAK
    assert.equal(event._attributes.type, 'a-f-X-i-g-a');
    assert.equal(event._attributes.stale, STALE);
    assert.equal(event.point._attributes.lon, 170.5);
    assert.equal(event.point._attributes.lat, -43.5);
    assert.ok(event.detail.usericon, 'icon is carried on the marker');
    assert.strictEqual(event.detail.marti?._attributes?.archive, false);

    const polyline = event.detail.shape?.polyline;
    assert.ok(polyline, 'detail.shape.polyline is set');
    assert.equal(polyline._attributes?.closed, true);
    assert.equal(polyline.vertex.length, RING.length);
});
