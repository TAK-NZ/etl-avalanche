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
                return Type.Object({});
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

            // Calculate expiry time
            const created = new Date(forecast.created);
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
                start: forecast.created,
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

            const features: any[] = [];

            for (const regionId of VALID_REGIONS) {
                const regionInfo = await this.getRegionInfo(regionId, env.Timeout);
                const data = await this.getForecastData(regionId, env.Timeout);
                
                if (!data || !regionInfo) {
                    console.warn(`No data for region ${regionId}`);
                    continue;
                }

                const startTime = this.parseDate(data.start);
                const expiresTime = data.expires ? this.parseDate(data.expires) : undefined;

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
                const baseProperties: Record<string, unknown> = {
                    callsign: `Avalanche Risk: ${regionInfo.title} - ${data.levelText}`,
                    type: 'a-o-X-i-g-h',
                    time: startTime,
                    start: startTime,
                    stale: expiresTime,
                    remarks: [
                        `Avalanche Risk: ${regionInfo.title} - ${data.levelText}`,
                        `Location: ${regionInfo.title}`,
                        `Danger Level: ${data.levelText}`,
                        `Description: ${data.description}`,
                        `Issued: ${data.start}`,
                        ...(data.expires ? [`Valid Until: ${data.expires}`] : [])
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
                features.push({
                    id: `avalanche-${regionId}-center`,
                    type: 'Feature',
                    properties: {
                        ...baseProperties,
                        icon: AVALANCHE_ICONS[data.level] || AVALANCHE_ICONS[0]
                    },
                    geometry: {
                        type: 'Point',
                        coordinates: [regionInfo.latitude, regionInfo.longitude]
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