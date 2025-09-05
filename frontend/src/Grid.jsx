import React from 'react';
import Team from './Team';

export default function Grid({ onPlayersChange }) {
  // Generate 24 lines from $230M to $0 in $10M increments (reversed)
  const salaryLines = Array.from({ length: 24 }, (_, index) => {
    const amount = (23 - index) * 10000000; // Start from $230M and go down to $0
    return {
      amount,
      formattedAmount: amount === 0 ? '$0' : `$${(amount / 1000000).toFixed(0)}M`
    };
  });

  // NBA salary thresholds
  const salaryThresholds = [
    { amount: 139182000, label: 'Salary Floor', formattedAmount: '$139.2M' },
    { amount: 154647000, label: 'Salary Cap', formattedAmount: '$154.6M' },
    { amount: 187895000, label: 'Luxury Tax', formattedAmount: '$187.9M' },
    { amount: 195945000, label: 'First Apron', formattedAmount: '$195.9M' },
    { amount: 207824000, label: 'Second Apron', formattedAmount: '$207.8M' }
  ];

  // Calculate position for salary thresholds
  const getThresholdPosition = (amount) => {
    const salaryInMillions = amount / 10000000;
    const index = 23 - salaryInMillions;
    const topPosition = (index / 23) * 100;
    return topPosition - (0.05 / 23) * 100; // Same offset as player cards
  };

  return (
    <div style={{width: '100%', display: 'flex', justifyContent: 'center', alignItems: 'center'}}>
      <div style={{position: 'relative', width: '100%', height: '89vh', minHeight: '590px'}}>
        {/* Container for the grid lines - centered */}
        <div style={{position: 'relative', width: '100%', height: '100%', display: 'flex', justifyContent: 'center'}}>
          <div style={{width: '100%', position: 'relative'}}>
            {salaryLines.map((line, index) => {

              // First line at top (0), last line at bottom (85vh)

              const topPosition = (index / (salaryLines.length - 1)) * 100;
              
              return (
                <div
                  key={index}
                  style={{ position: 'absolute', width: '100%', display: 'flex', alignItems: 'center', top: `${topPosition}%` }}
                >
                  {/* Three-container layout with proper centering */}
                  <div style={{width: '100%', display: 'flex', justifyContent: 'center'}}>
                    <div style={{display: 'flex', alignItems: 'center', width: '60%'}}>
                      {/* Left container - Money amount */}
                      <div style={{width: '5%', display: 'flex', justifyContent: 'flex-end', fontSize: '14px', paddingRight: '10px'}}>
                        <span style={{fontSize: '14px', fontWeight: '500', color: '#374151', whiteSpace: 'nowrap'}}>
                          {line.formattedAmount}
                        </span>
                      </div>
                      
                      {/* Middle container - Line */}
                      <div style={{width: '90%', display: 'flex', justifyContent: 'center'}}>
                        <div style={{width: '100%', height: '1px', backgroundColor: '#000000'}} />
                      </div>
                      
                      {/* Right container - Placeholder */}
                      <div style={{width: '5%', display: 'flex', justifyContent: 'flex-start', paddingLeft: '8px'}}>
                        {/* Placeholder content can go here */}
                      </div>
                    </div>
                  </div>
                </div>
              );
            })}
            
            {/* NBA Salary Threshold Lines */}
            {salaryThresholds.map((threshold, index) => (
              <div
                key={`threshold-${index}`}
                style={{ 
                  position: 'absolute', 
                  width: '100%', 
                  display: 'flex', 
                  alignItems: 'center', 
                  top: `${getThresholdPosition(threshold.amount)}%` 
                }}
              >
                <div style={{width: '100%', display: 'flex', justifyContent: 'center'}}>
                  <div style={{display: 'flex', alignItems: 'center', width: '60%'}}>
                    {/* Left container - Empty space */}
                    <div style={{width: '5%', display: 'flex', justifyContent: 'flex-end', paddingRight: '10px'}}>
                      {/* Empty space */}
                    </div>
                    
                    {/* Middle container - Line */}
                    <div style={{width: '90%', display: 'flex', justifyContent: 'center'}}>
                      <div style={{width: '100%', height: '0px', borderTop: '1px dotted #000000'}} />
                    </div>
                    
                    {/* Right container - Threshold label */}
                    <div style={{width: '5%', display: 'flex', justifyContent: 'flex-start', paddingLeft: '8px'}}>
                      <div style={{display: 'flex', flexDirection: 'column', alignItems: 'flex-start'}}>
                        <span style={{fontSize: '12px', fontWeight: '600', color: '#000000', whiteSpace: 'nowrap'}}>
                          {threshold.label}
                        </span>
                        <span style={{fontSize: '10px', color: '#000000', whiteSpace: 'nowrap', marginTop: '-4px'}}>
                          {threshold.formattedAmount}
                        </span>
                      </div>
                    </div>
                  </div>
                </div>
              </div>
            ))}
            
            {/* Team players overlay - positioned after salary lines */}
            <Team onPlayersChange={onPlayersChange} />
          </div>
        </div>
      </div>
    </div>
  );
}

