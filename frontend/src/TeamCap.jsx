import React from 'react';

const TeamCap = ({ players }) => {
  // Calculate total salary
  const totalSalary = players.reduce((sum, player) => sum + player.salary, 0);
  
  // Format the total salary
  const formatSalary = (salary) => {
    if (salary >= 1000000000) {
      return `$${(salary / 1000000000).toFixed(1)}B`;
    } else if (salary >= 1000000) {
      return `$${(salary / 1000000).toFixed(0)}M`;
    } else if (salary >= 1000) {
      return `$${(salary / 1000).toFixed(0)}K`;
    } else {
      return `$${salary.toFixed(0)}`;
    }
  };

  if (!players || players.length === 0) return null;

  return (
    <div 
      className="absolute left-1/2 transform -translate-x-1/2 bg-white border-2 border-blue-500 rounded-lg shadow-lg px-2 py-3"
      style={{
        zIndex: 45,
        top: '1vh'
      }}
    >
      <div className="text-center">
        <div className="text-sm text-gray-600 mb-1">Total Team Salary</div>
        <div className="text-2xl font-bold text-gray-800">
          {formatSalary(totalSalary)}
        </div>
      </div>
    </div>
  );
};

export default TeamCap;
