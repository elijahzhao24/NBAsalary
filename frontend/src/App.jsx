import { BrowserRouter as Router } from 'react-router-dom'
import { useState, useCallback } from 'react'
import Grid from './Grid'
import Header from './Header'
import TeamCap from './TeamCap'

function App() {
  const [players, setPlayers] = useState([]);

  const handlePlayersChange = useCallback((newPlayers) => {
    setPlayers(newPlayers);
  }, []);

  return (
    <Router>
      <div style={{minHeight: '100vh', width: '100%', position: 'relative'}}>
        <Header />
        <TeamCap players={players} />
        <Grid onPlayersChange={handlePlayersChange} />
      </div>
    </Router>
  )
}

export default App
