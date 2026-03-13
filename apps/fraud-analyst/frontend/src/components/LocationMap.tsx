import { Map, Marker } from 'pigeon-maps'

function darkTileProvider(x: number, y: number, z: number): string {
  return `https://basemaps.cartocdn.com/dark_all/${z}/${x}/${y}@2x.png`
}

interface LocationMapProps {
  lat: number | null
  lng: number | null
  region?: string | null
  state?: string | null
}

export default function LocationMap({ lat, lng, region, state }: LocationMapProps) {
  if (lat === null || lng === null || isNaN(lat) || isNaN(lng)) {
    return (
      <div className="h-48 bg-gray-800/50 rounded flex items-center justify-center text-xs text-gray-500">
        No location data available
      </div>
    )
  }

  return (
    <div className="rounded overflow-hidden border border-gray-700">
      <Map
        provider={darkTileProvider}
        defaultCenter={[lat, lng]}
        defaultZoom={6}
        height={200}
        attribution={false}
        dprs={[1, 2]}
      >
        <Marker width={36} anchor={[lat, lng]} color="#ef4444" />
      </Map>
      {(region || state) && (
        <div className="bg-gray-800/80 px-2 py-1 text-[10px] text-gray-400 flex gap-3">
          {region && <span>Region: {region}</span>}
          {state && <span>State: {state}</span>}
        </div>
      )}
    </div>
  )
}
