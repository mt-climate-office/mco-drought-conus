import { createStorageBrowser } from '@aws-amplify/ui-react-storage/browser'
import { fetchAuthSession } from 'aws-amplify/auth'

const BUCKET = import.meta.env.VITE_S3_BUCKET
const REGION = import.meta.env.VITE_AWS_REGION

const { StorageBrowser } = createStorageBrowser({
  config: {
    region: REGION,

    // Expose all of derived/conus_drought/ (latest/ and date-stamped archives).
    listLocations: async () => ({
      items: [{
        bucket: BUCKET,
        id: `${BUCKET}/derived/conus_drought/`,
        permissions: ['get', 'list'],
        prefix: 'derived/conus_drought/',
        type: 'PREFIX',
      }],
      nextToken: undefined,
    }),

    // Provide Cognito guest credentials for every S3 call.
    getLocationCredentials: async () => {
      const session = await fetchAuthSession()
      return { credentials: session.credentials }
    },

    // No auth state changes for public/guest access.
    registerAuthListener: (_listener) => {},
  },
})

export default function App() {
  return (
    <div style={{ height: '100dvh', display: 'flex', flexDirection: 'column' }}>
      <header style={{
        padding: '0.75rem 1.5rem',
        background: '#1a3a5c',
        color: 'white',
        display: 'flex',
        alignItems: 'center',
        gap: '0.75rem',
        flexShrink: 0,
      }}>
        <img
          src="https://climate.umt.edu/img/MCO_logo_white.svg"
          alt="Montana Climate Office"
          style={{ height: '2rem' }}
          onError={(e) => { e.target.style.display = 'none' }}
        />
        <span style={{ fontSize: '1.1rem', fontWeight: 600 }}>
          MCO Drought Data Browser
        </span>
      </header>
      <main style={{ flex: 1, overflow: 'hidden' }}>
        <StorageBrowser />
      </main>
    </div>
  )
}
