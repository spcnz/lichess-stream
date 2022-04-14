import { useEffect, useState } from "react";
import { Chessboard } from "react-chessboard";
import Timer from "./Timer";
import axios from "axios";
import Stats from "./Stats";

const blackTimerStyle = {
  borderStyle: 'solid',
  width: '150px',
  height: '50px',
  fontSize: '50px',
  textAlign: 'center',
  marginLeft: '250px'
}


const whiteTimerStyle = {
  borderStyle: 'solid',
  width: '150px',
  height: '50px',
  fontSize: '50px',
  textAlign: 'center',
  position: 'absolute',
  right:' 610px'
}


const Board = () => {
  const [position, setPosition] = useState({"game_id": "vyg6shq8", "fen": "3r1rk1/1bq2pbp/ppN1pnp1/3p4/P1P1P3/2N1BP2/1P2B1PP/R2RQ1K1 b", "lm": "d4c6", "wc": 524, "bc": 535, "next_best1": "d4c6", "next_best2": "e1g3", "evaluation": 66});
  const [moves, setMoves] = useState([])

  const getGameStatus = async () => {
    const { data } = await axios.get(
      `http://localhost:${process.env.REACT_APP_SERVER_PORT}`
    );
    if (data) {
      console.log(data)
      console.log("razlikuju se : ", data.fen === position.fen)
      if (data.fen != position.fen) {
        setPosition(data)
        setMoves(prevState => ([...prevState, data]))
      }
    }
    setTimeout(getGameStatus, 500);
  };

  useEffect(() => {
    getGameStatus();
  }, [])

  return (
    <div>
      <div  style={{ margin: 'auto', width: '50%'}}>
        <Chessboard position={position?.fen} />
      </div>
        <h2>Last move: {position?.lm} Next best moves are: {position?.next_best1} {position?.next_best2}</h2>
    </div>
  )
}

export default Board;