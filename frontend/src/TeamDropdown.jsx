import React from 'react';

const teams = [
  { id: 1, code: "GSW", name: "Golden State Warriors" },
  { id: 2, code: "PHI", name: "Philadelphia 76ers" },
  { id: 3, code: "DEN", name: "Denver Nuggets" },
  { id: 4, code: "MIL", name: "Milwaukee Bucks" },
  { id: 5, code: "DAL", name: "Dallas Mavericks" },
  { id: 6, code: "BOS", name: "Boston Celtics" },
  { id: 7, code: "HOU", name: "Houston Rockets" },
  { id: 8, code: "PHX", name: "Phoenix Suns" },
  { id: 9, code: "NYK", name: "New York Knicks" },
  { id: 10, code: "LAL", name: "Los Angeles Lakers" },
  { id: 11, code: "LAC", name: "Los Angeles Clippers" },
  { id: 12, code: "DET", name: "Detroit Pistons" },
  { id: 13, code: "UTA", name: "Utah Jazz" },
  { id: 14, code: "CLE", name: "Cleveland Cavaliers" },
  { id: 15, code: "SAC", name: "Sacramento Kings" },
  { id: 16, code: "ATL", name: "Atlanta Hawks" },
  { id: 17, code: "MIN", name: "Minnesota Timberwolves" },
  { id: 18, code: "IND", name: "Indiana Pacers" },
  { id: 19, code: "MEM", name: "Memphis Grizzlies" },
  { id: 20, code: "NOP", name: "New Orleans Pelicans" },
  { id: 21, code: "TOR", name: "Toronto Raptors" },
  { id: 22, code: "ORL", name: "Orlando Magic" },
  { id: 23, code: "OKC", name: "Oklahoma City Thunder" },
  { id: 24, code: "BKN", name: "Brooklyn Nets" },
  { id: 25, code: "CHA", name: "Charlotte Hornets" },
  { id: 26, code: "MIA", name: "Miami Heat" },
  { id: 27, code: "SAS", name: "San Antonio Spurs" },
  { id: 28, code: "WAS", name: "Washington Wizards" },
  { id: 29, code: "POR", name: "Portland Trail Blazers" },
  { id: 30, code: "CHI", name: "Chicago Bulls" }
];

export default function TeamDropdown({ isOpen, onSelectTeam, onClose }) {
  if (!isOpen) return null;

  const handleTeamSelect = (team) => {
    onSelectTeam(team);
    onClose();
  };

  return (
    <div style={{position: 'fixed', top: 0, left: 0, right: 0, bottom: 0, backgroundColor: 'rgba(0, 0, 0, 0.5)', zIndex: 50, display: 'flex', alignItems: 'flex-start', justifyContent: 'flex-end', paddingTop: '80px', paddingRight: '24px'}}>
      <div style={{backgroundColor: 'white', borderRadius: '8px', boxShadow: '0 25px 50px -12px rgba(0, 0, 0, 0.25)', width: '70%', maxWidth: '72rem', maxHeight: '80vh', overflowY: 'auto', border: '1px solid #e5e7eb'}}>
        <div style={{padding: '24px'}}>
          <div style={{display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '24px'}}>
            <h2 style={{fontSize: '24px', fontWeight: 'bold', color: '#1f2937'}}>Select a Team</h2>
            <button 
              onClick={onClose}
              style={{color: '#6b7280', fontSize: '24px', fontWeight: 'bold', width: '32px', height: '32px', display: 'flex', alignItems: 'center', justifyContent: 'center', backgroundColor: 'transparent', border: 'none', cursor: 'pointer'}}
            >
              ×
            </button>
          </div>
          
          <div style={{display: 'grid', gridTemplateColumns: 'repeat(2, 1fr)', gap: '16px', paddingLeft: '16px', paddingRight: '16px', paddingBottom: '16px'}}>
            {teams.map((team) => (
              <button
                key={team.id}
                onClick={() => handleTeamSelect(team)}
                style={{padding: '16px', border: '1px solid #e5e7eb', borderRadius: '8px', backgroundColor: 'white', cursor: 'pointer', textAlign: 'center', transition: 'all 0.2s'}}
                onMouseEnter={(e) => {
                  e.target.style.backgroundColor = '#eff6ff';
                  e.target.style.borderColor = '#999999';
                }}
                onMouseLeave={(e) => {
                  e.target.style.backgroundColor = 'white';
                  e.target.style.borderColor = '#e5e7eb';
                }}
              >
                <div style={{fontWeight: 'bold', fontSize: '18px', color: '#1f2937'}}>
                  {team.code}
                </div>
                <div style={{fontSize: '14px', color: '#6b7280', marginTop: '4px'}}>
                  {team.name}
                </div>
              </button>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}
