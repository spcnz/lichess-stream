const Stats = ({ moves }) => {
    return (
        <div style={{margin: '100px'}}>
            <table style={{ border: '1px solid #333'}}>
                <tr>
                    <th>Game ID</th>
                    <th>Last move</th>
                    <th>Next best move</th>
                    <th>Next best move</th>

                </tr>
                {moves.map(move => 
                    <tr>
                        <td>{move.game_id}</td>
                        <td>{move.lm}</td>
                        <td>{move.next_best1}</td>
                        <td>{move.next_best2}</td>
                    </tr>
                )}
            </table>
        </div>
    )
}


export default Stats;
