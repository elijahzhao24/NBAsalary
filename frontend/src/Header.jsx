import React, { useState } from 'react';
import { useNavigate, useLocation } from 'react-router-dom';
import Logo from './assets/logo.png';
import LogoText from './assets/fulllogo.png';
import { FiGithub, FiChevronDown } from 'react-icons/fi';
import TeamDropdown from './TeamDropdown';

export default function Header() {
    const [isDropdownOpen, setIsDropdownOpen] = useState(false);
    const navigate = useNavigate();
    const location = useLocation();
    
    // Get current team from URL
    const currentTeamCode = location.pathname === '/' ? null : location.pathname.slice(1);
    
    // Team data to get team name from code
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
    
    const currentTeam = teams.find(team => team.code === currentTeamCode);

    const handleTeamSelect = (team) => {
        navigate(`/${team.code}`);
        console.log('Selected team:', team);
    };

    return (    
    <>
        <div style={{display: 'flex', alignItems: 'center', justifyContent: 'space-between', padding: '0 24px', height: '8vh', minHeight: '50px', position: 'relative'}}>
            <div style={{display: 'flex', alignItems: 'center'}}>
                <button 
                    onClick={() => navigate('/')}
                    style={{backgroundColor: 'transparent', border: 'none', cursor: 'pointer', padding: '0'}}
                >
                    <img src={LogoText} alt="Logo" style={{height: '40px', maxHeight: '40px'}} />
                </button>
            </div>
            
            <div style={{display: 'flex', alignItems: 'center', gap: '16px'}}>
                <button 
                    onClick={() => setIsDropdownOpen(true)}
                    style={{display: 'flex', alignItems: 'center', gap: '8px', padding: '8px 16px', backgroundColor: '#2563eb', color: 'white', borderRadius: '8px', border: 'none', cursor: 'pointer'}}
                >
                    {currentTeam ? currentTeam.code : 'Select Team'}
                    <FiChevronDown style={{width: '16px', height: '16px'}} />
                </button>

                <button 
                    onClick={() => window.open('https://github.com/elijahzhao24/NBAsalary')}
                    style={{padding: '8px', backgroundColor: 'transparent', border: 'none', cursor: 'pointer', borderRadius: '8px'}}>
                        <FiGithub style={{color: '#bbbbbb', width: '24px', height: '24px'}}/>
                </button>
            </div>
        </div>
        
        <TeamDropdown 
            isOpen={isDropdownOpen}
            onSelectTeam={handleTeamSelect}
            onClose={() => setIsDropdownOpen(false)}
        />
    </>
    )
}
