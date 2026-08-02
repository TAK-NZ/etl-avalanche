import { Type, TSchema } from '@sinclair/typebox';
import { fetch } from '@tak-ps/etl';
import ETL, { Event, SchemaType, handler as internal, local, InvocationType, DataFlowType } from '@tak-ps/etl';

// Avalanche danger level icon mapping
const AVALANCHE_ICONS: Record<number, string> = {
    0: 'bb4df0a6-ca8d-4ba8-bb9e-3deb97ff015e:NaturalHazards/NH.41B.Avalanche.DangerLevel0.Label.png',
    1: 'bb4df0a6-ca8d-4ba8-bb9e-3deb97ff015e:NaturalHazards/NH.42B.Avalanche.DangerLevel1.Label.png',
    2: 'bb4df0a6-ca8d-4ba8-bb9e-3deb97ff015e:NaturalHazards/NH.43B.Avalanche.DangerLevel2.Label.png',
    3: 'bb4df0a6-ca8d-4ba8-bb9e-3deb97ff015e:NaturalHazards/NH.44B.Avalanche.DangerLevel3.Label.png',
    4: 'bb4df0a6-ca8d-4ba8-bb9e-3deb97ff015e:NaturalHazards/NH.45B.Avalanche.DangerLevel4and5.Label.png',
    5: 'bb4df0a6-ca8d-4ba8-bb9e-3deb97ff015e:NaturalHazards/NH.45B.Avalanche.DangerLevel4and5.Label.png'
};

const AVALANCHE_COLORS: Record<number, string> = {
    5: 'rgb(0, 0, 0)',
    4: 'rgb(239, 43, 47)',
    3: 'rgb(248, 151, 44)',
    2: 'rgb(255, 244, 31)',
    1: 'rgb(84, 187, 81)',
    0: 'rgb(128, 128, 128)'
};

const VALID_REGIONS = [1, 2, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 15];

// NZ timezone formatters. `timeZone: 'Pacific/Auckland'` handles the NZST/NZDT
// daylight saving transition automatically - do not hardcode a +12/+13 offset.
const NZ_DATE_FORMAT = new Intl.DateTimeFormat('en-NZ', {
    timeZone: 'Pacific/Auckland',
    day: '2-digit', month: '2-digit', year: 'numeric'
});
const NZ_TIME_FORMAT = new Intl.DateTimeFormat('en-NZ', {
    timeZone: 'Pacific/Auckland',
    hour: '2-digit', minute: '2-digit', hour12: false
});
const NZ_TZ_NAME_FORMAT = new Intl.DateTimeFormat('en-NZ', {
    timeZone: 'Pacific/Auckland',
    timeZoneName: 'short'
});

/**
 * Floored relative time between `target` and `reference`, e.g. "3 hours ago"
 * or "in 2 days". Uses floor (not round) so an event doesn't jump to the next
 * unit a few seconds after crossing the boundary.
 */
function formatRelativeTime(target: Date, reference: Date): string {
    const diffMs = reference.getTime() - target.getTime();
    const isPast = diffMs >= 0;
    const absMs = Math.abs(diffMs);

    const minutes = Math.floor(absMs / (60 * 1000));
    const hours = Math.floor(absMs / (60 * 60 * 1000));
    const days = Math.floor(absMs / (24 * 60 * 60 * 1000));

    let value: number;
    let unit: string;
    if (hours < 1) {
        value = minutes;
        unit = 'minute';
    } else if (hours < 24) {
        value = hours;
        unit = 'hour';
    } else {
        value = days;
        unit = 'day';
    }

    const label = `${value} ${unit}${value === 1 ? '' : 's'}`;
    return isPast ? `${label} ago` : `in ${label}`;
}

const NZ_LONG_OFFSET_FORMAT = new Intl.DateTimeFormat('en-NZ', {
    timeZone: 'Pacific/Auckland',
    timeZoneName: 'longOffset'
});

/** Returns the Pacific/Auckland UTC offset, in minutes, at the given instant. */
function getAucklandOffsetMinutes(date: Date): number {
    const offsetPart = NZ_LONG_OFFSET_FORMAT.formatToParts(date)
        .find((part) => part.type === 'timeZoneName')?.value ?? 'GMT+12:00';
    const match = offsetPart.match(/GMT([+-])(\d{2}):(\d{2})/);
    if (!match) return 12 * 60;
    const [, sign, hours, minutes] = match;
    return (sign === '-' ? -1 : 1) * (Number(hours) * 60 + Number(minutes));
}

/**
 * Parses the upstream API's naive `YYYY-MM-DD HH:MM:SS` timestamps (no
 * timezone designator, confirmed to be NZ local time, not UTC) into a
 * correct UTC `Date`. Resolves the NZST/NZDT offset via `Intl.DateTimeFormat`
 * rather than a hardcoded +12/+13, so it stays correct across DST transitions.
 */
function parseNZNaiveDateTime(naiveStr: string): Date {
    const match = naiveStr.match(/^(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2}):(\d{2})/);
    if (!match) return new Date(naiveStr);
    const [, year, month, day, hour, minute, second] = match;
    const naiveUtcMs = Date.UTC(
        Number(year), Number(month) - 1, Number(day),
        Number(hour), Number(minute), Number(second)
    );
    // Resolve offset using the naive instant, then re-resolve using the
    // corrected instant to handle the (rare) case where the naive time
    // falls right on a DST boundary.
    const offset1 = getAucklandOffsetMinutes(new Date(naiveUtcMs));
    const offset2 = getAucklandOffsetMinutes(new Date(naiveUtcMs - offset1 * 60000));
    return new Date(naiveUtcMs - offset2 * 60000);
}

/**
 * Formats an ISO 8601 UTC timestamp as NZ local time, human formatted:
 * `DD/MM/YYYY, HH:mm <NZST|NZDT> (<relative time>)`
 */
function formatNZLocal(isoString: string, reference: Date): string {
    const date = new Date(isoString);
    const datePart = NZ_DATE_FORMAT.format(date);
    const timePart = NZ_TIME_FORMAT.format(date);
    const tzPart = NZ_TZ_NAME_FORMAT.formatToParts(date)
        .find((part) => part.type === 'timeZoneName')?.value ?? '';
    const relative = formatRelativeTime(date, reference);
    return `${datePart}, ${timePart} ${tzPart} (${relative})`;
}

interface RegionInfo {
    id: number;
    title: string;
    latitude: number;
    longitude: number;
    geometry: string;
}

const Env = Type.Object({
    'Timeout': Type.Number({
        description: 'Request timeout in milliseconds',
        default: 30000
    })
});

const AvalancheProperties = Type.Object({
    dangerLevel: Type.Number({
        description: 'Avalanche danger level (0-5)'
    }),
    dangerLevelText: Type.String({
        description: 'Human readable danger level'
    }),
    region: Type.String({
        description: 'Avalanche region name'
    }),
    regionId: Type.Number({
        description: 'Avalanche region ID'
    }),
    description: Type.String({
        description: 'Forecast description'
    }),
    issuedUTC: Type.String({
        description: 'Forecast issue time, raw ISO 8601 UTC string'
    }),
    issuedLocal: Type.String({
        description: 'Forecast issue time, NZ local time, human formatted'
    }),
    expiresUTC: Type.Optional(Type.String({
        description: 'Forecast expiry time, raw ISO 8601 UTC string'
    })),
    expiresLocal: Type.Optional(Type.String({
        description: 'Forecast expiry time, NZ local time, human formatted'
    }))
});

interface AvalancheData {
    location: string;
    level: number;
    levelText: string;
    description: string;
    start: string;
    expires: string;
    url: string;
}

export default class Task extends ETL {
    static name = 'etl-avalanche';
    static flow = [DataFlowType.Incoming];
    static invocation = [InvocationType.Schedule];

    async schema(
        type: SchemaType = SchemaType.Input,
        flow: DataFlowType = DataFlowType.Incoming
    ): Promise<TSchema> {
        if (flow === DataFlowType.Incoming) {
            if (type === SchemaType.Input) {
                return Env;
            } else {
                return AvalancheProperties;
            }
        } else {
            return Type.Object({});
        }
    }

    private async getRegionInfo(regionId: number, timeout: number): Promise<RegionInfo | null> {
        try {
            const url = `https://www.avalanche.net.nz/api/region/${regionId}`;
            const controller = new AbortController();
            const timeoutId = setTimeout(() => controller.abort(), timeout);

            const response = await fetch(url, {
                signal: controller.signal,
                headers: {
                    'User-Agent': 'Mozilla/5.0 (compatible; TAK-NZ-ETL/1.0)'
                }
            });

            clearTimeout(timeoutId);

            if (!response.ok) {
                console.warn(`Failed to fetch region info ${regionId}: ${response.status}`);
                return null;
            }

            const data = await response.json() as {
                id: number;
                title: string;
                latitude: number;
                longitude: number;
                geometry: string;
            };
            return {
                id: data.id,
                title: data.title,
                latitude: data.latitude,
                longitude: data.longitude,
                geometry: data.geometry
            };

        } catch (error) {
            console.error(`Error fetching region info ${regionId}:`, error);
            return null;
        }
    }

    private async getForecastData(regionId: number, timeout: number): Promise<AvalancheData | null> {
        try {
            const url = `https://www.avalanche.net.nz/api/forecast`;
            const controller = new AbortController();
            const timeoutId = setTimeout(() => controller.abort(), timeout);

            const response = await fetch(url, {
                signal: controller.signal,
                headers: {
                    'User-Agent': 'Mozilla/5.0 (compatible; TAK-NZ-ETL/1.0)',
                    'Accept': 'application/json'
                }
            });

            clearTimeout(timeoutId);

            if (!response.ok) {
                console.warn(`Failed to fetch forecasts: ${response.status}`);
                return null;
            }

            const data = await response.json() as {
                forecasts: {
                    regionId: number;
                    altitudeDanger: { rating: number; description: string }[];
                    created: string;
                    validPeriod: string;
                    importantInformation: string;
                }[];
            };
            
            if (!data.forecasts || data.forecasts.length === 0) {
                console.warn(`No forecasts available`);
                return null;
            }

            // Filter forecasts for this region and get the most recent one
            const regionForecasts = data.forecasts.filter(f => f.regionId === regionId);
            if (regionForecasts.length === 0) {
                console.warn(`No forecasts available for region ${regionId}`);
                return null;
            }

            // Get the most recent forecast (they should be sorted by creation date)
            const forecast = regionForecasts[0];
            
            // Calculate overall danger level (use highest altitude rating)
            let maxRating = 0;
            let ratingDescription = 'No rating available';
            
            if (forecast.altitudeDanger && forecast.altitudeDanger.length > 0) {
                for (const altitude of forecast.altitudeDanger) {
                    if (altitude.rating > maxRating && altitude.rating > 0) {
                        maxRating = altitude.rating;
                        ratingDescription = altitude.description;
                    }
                }
            }

            // Handle special case for insufficient snow
            if (maxRating === 0) {
                const insufficientSnow = forecast.altitudeDanger?.find(a => a.rating === -2);
                if (insufficientSnow) {
                    maxRating = 0;
                    ratingDescription = insufficientSnow.description;
                }
            }

            // `forecast.created` is a naive NZ local timestamp (no timezone
            // designator) - convert it to a proper UTC instant before doing
            // any arithmetic or exposing it as an ISO 8601 UTC string.
            const created = parseNZNaiveDateTime(forecast.created);
            const validHours = forecast.validPeriod === '48hrs' ? 48 : 24;
            const expires = new Date(created.getTime() + validHours * 60 * 60 * 1000);

            // Use important information if available, otherwise fall back to rating description
            const description = forecast.importantInformation ? 
                forecast.importantInformation.replace(/<[^>]*>/g, '').trim() : 
                ratingDescription;

            return {
                location: `Region ${regionId}`,
                level: Math.max(0, maxRating), // Ensure non-negative
                levelText: this.getDangerLevelText(maxRating),
                description,
                start: created.toISOString(),
                expires: expires.toISOString(),
                url: `https://www.avalanche.net.nz/region/${regionId}`
            };

        } catch (error) {
            console.error(`Error fetching forecast ${regionId}:`, error);
            return null;
        }
    }

    private getDangerLevelText(rating: number): string {
        switch (rating) {
            case -2: return 'Insufficient Snow';
            case 0: return 'No Rating';
            case 1: return 'Low (1)';
            case 2: return 'Moderate (2)';
            case 3: return 'Considerable (3)';
            case 4: return 'High (4)';
            case 5: return 'Extreme (5)';
            default: return `Level ${rating}`;
        }
    }

    private calculatePolygonCentroid(coordinates: number[][][]): [number, number] {
        const points = coordinates[0];
        if (points.length < 3) {
            return [0, 0];
        }
        
        let area = 0;
        let cx = 0;
        let cy = 0;
        
        for (let i = 0; i < points.length - 1; i++) {
            const x0 = points[i][0];
            const y0 = points[i][1];
            const x1 = points[i + 1][0];
            const y1 = points[i + 1][1];
            
            const a = x0 * y1 - x1 * y0;
            area += a;
            cx += (x0 + x1) * a;
            cy += (y0 + y1) * a;
        }
        
        area *= 0.5;
        if (Math.abs(area) < 1e-10) {
            let x = 0, y = 0;
            for (const point of points) {
                x += point[0];
                y += point[1];
            }
            return [x / points.length, y / points.length];
        }
        
        cx /= (6 * area);
        cy /= (6 * area);
        
        return [cx, cy];
    }

    private validateCoordinates(lat: number, lon: number): [number, number] {
        if (lat < -90 || lat > 90) {
            if (lon >= -90 && lon <= 90) {
                return [lat, lon];
            }
        }
        
        if (lon < -180 || lon > 180) {
            if (lat >= -180 && lat <= 180) {
                return [lon, lat];
            }
        }
        
        return [lon, lat];
    }

    private parseDate(dateStr: string): string {
        try {
            // Try to parse various date formats
            const date = new Date(dateStr);
            if (!isNaN(date.getTime())) {
                return date.toISOString();
            }
            
            // Fallback for NZ date formats like "Monday 1st September 2025, 15:18"
            const nzMatch = dateStr.match(/(\w+)\s+(\d+)\w*\s+(\w+)\s+(\d{4}),\s*(\d{1,2}):(\d{2})/);
            if (nzMatch) {
                const [, , day, month, year, hour, minute] = nzMatch;
                const monthMap: Record<string, number> = {
                    'January': 0, 'February': 1, 'March': 2, 'April': 3,
                    'May': 4, 'June': 5, 'July': 6, 'August': 7,
                    'September': 8, 'October': 9, 'November': 10, 'December': 11
                };
                const monthNum = monthMap[month];
                if (monthNum !== undefined) {
                    const parsedDate = new Date(parseInt(year), monthNum, parseInt(day), parseInt(hour), parseInt(minute));
                    return parsedDate.toISOString();
                }
            }
            
            return new Date().toISOString();
        } catch {
            return new Date().toISOString();
        }
    }

    async control() {
        try {
            const env = await this.env(Env);
            console.log('ok - Starting avalanche data scraping');

            const features: Array<{
                id: string;
                type: 'Feature';
                properties: Record<string, unknown>;
                geometry: { type: 'Point'; coordinates: number[] } | { type: 'Polygon'; coordinates: number[][][] };
            }> = [];

            for (const regionId of VALID_REGIONS) {
                const regionInfo = await this.getRegionInfo(regionId, env.Timeout);
                const data = await this.getForecastData(regionId, env.Timeout);
                
                if (!data || !regionInfo) {
                    console.warn(`No data for region ${regionId}`);
                    continue;
                }

                // Use ETL run time for CoT time/start, and now+24h for stale.
                // The upstream forecast's created timestamp may be many hours old, which
                // would produce a stale value already in the past and cause ATAK to show
                // items as expired. The ETL runs on a schedule so stale just needs to
                // outlive the interval between runs.
                const now = new Date();
                const cotTime = now.toISOString();
                const cotStale = new Date(now.getTime() + 24 * 60 * 60 * 1000).toISOString();

                // Parse region geometry
                let polygonCoordinates: number[][][] | null = null;
                try {
                    const geometryData = JSON.parse(regionInfo.geometry);
                    if (geometryData.layers && geometryData.layers[0] && geometryData.layers[0].geometry) {
                        const geom = geometryData.layers[0].geometry;
                        if (geom.type === 'Polygon' && geom.coordinates) {
                            polygonCoordinates = geom.coordinates;
                        }
                    }
                } catch (error) {
                    console.warn(`Failed to parse geometry for region ${regionId}:`, error);
                }

                const color = AVALANCHE_COLORS[data.level] || AVALANCHE_COLORS[0];

                // issued/expires: raw UTC ISO strings unmodified, plus NZ local
                // human-formatted equivalents (relative time computed against now).
                const issuedUTC = data.start;
                const issuedLocal = formatNZLocal(data.start, now);
                const expiresUTC = data.expires || undefined;
                const expiresLocal = data.expires ? formatNZLocal(data.expires, now) : undefined;

                const baseProperties: Record<string, unknown> = {
                    callsign: `Avalanche Risk: ${regionInfo.title} - ${data.levelText}`,
                    type: 'a-f-X-i-g-a',
                    time: cotTime,
                    start: cotTime,
                    stale: cotStale,
                    dangerLevel: data.level,
                    dangerLevelText: data.levelText,
                    region: regionInfo.title,
                    regionId: regionId,
                    description: data.description,
                    issuedUTC,
                    issuedLocal,
                    ...(expiresUTC ? { expiresUTC } : {}),
                    ...(expiresLocal ? { expiresLocal } : {}),
                    metadata: {
                        dangerLevel: data.level,
                        dangerLevelText: data.levelText,
                        region: regionInfo.title,
                        regionId: regionId,
                        description: data.description,
                        issuedUTC,
                        issuedLocal,
                        ...(expiresUTC ? { expiresUTC } : {}),
                        ...(expiresLocal ? { expiresLocal } : {})
                    },
                    remarks: [
                        `Avalanche Risk: ${regionInfo.title} - ${data.levelText}`,
                        `Location: ${regionInfo.title}`,
                        `Danger Level: ${data.levelText}`,
                        `Description: ${data.description}`,
                        `Issued (NZ): ${issuedLocal}`,
                        ...(expiresLocal ? [`Expires (NZ): ${expiresLocal}`] : []),
                        `Issued (UTC): ${issuedUTC}`,
                        ...(expiresUTC ? [`Expires (UTC): ${expiresUTC}`] : [])
                    ].join('\n'),
                    links: [{
                        uid: `avalanche-${regionId}`,
                        relation: 'r-u',
                        mime: 'text/html',
                        url: data.url,
                        remarks: 'Avalanche Forecast Details'
                    }]
                };

                // Add polygon feature if geometry available
                if (polygonCoordinates) {
                    features.push({
                        id: `avalanche-${regionId}`,
                        type: 'Feature',
                        properties: {
                            ...baseProperties,
                            stroke: color,
                            'stroke-opacity': 0.4,
                            'stroke-width': 2,
                            'stroke-style': 'solid',
                            'fill-opacity': 0.4,
                            fill: color
                        },
                        geometry: {
                            type: 'Polygon',
                            coordinates: polygonCoordinates
                        }
                    });
                }

                // Add center point with icon
                const centerCoords = polygonCoordinates ? 
                    this.calculatePolygonCentroid(polygonCoordinates) :
                    this.validateCoordinates(regionInfo.latitude, regionInfo.longitude);
                    
                features.push({
                    id: `avalanche-${regionId}-center`,
                    type: 'Feature',
                    properties: {
                        ...baseProperties,
                        icon: AVALANCHE_ICONS[data.level] || AVALANCHE_ICONS[0],
                        links: [{
                            uid: `avalanche-${regionId}-center`,
                            relation: 'r-u',
                            mime: 'text/html',
                            url: data.url,
                            remarks: 'Avalanche Forecast Details'
                        }]
                    },
                    geometry: {
                        type: 'Point',
                        coordinates: centerCoords
                    }
                });

                console.log(`Added avalanche data for ${regionInfo.title} (Level ${data.level}) with ${polygonCoordinates ? 'polygon' : 'point only'}`);
            }

            const fc = {
                type: 'FeatureCollection' as const,
                features
            };

            console.log(`ok - Generated ${features.length} avalanche forecast features`);

            await this.submit(fc);
            console.log(`ok - submitted avalanche forecast data`);

        } catch (error) {
            console.error('Error in avalanche ETL:', error);
            throw error;
        }
    }
}

export async function handler(event: Event = {}) {
    return await internal(new Task(import.meta.url), event);
}

if (import.meta.url === `file://${process.argv[1]}`) {
    await local(new Task(import.meta.url), import.meta.url);
}