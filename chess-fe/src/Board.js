import { useEffect, useState } from "react";
import { Chessboard } from "react-chessboard";
import axios from "axios";

const Board = () => {
  const [position, setPosition] = useState("");

  const getGameStatus = async () => {
    const { data } = await axios.get(
      `http://localhost:${process.env.REACT_APP_SERVER_PORT}`
    );
    if (data) {
      console.log(data)
      setPosition(data.fen)
    }
    setTimeout(getGameStatus, 2000);
  };

  useEffect(() => {
    getGameStatus();
  }, [])

  return <Chessboard position={position} />;
}

export default Board;