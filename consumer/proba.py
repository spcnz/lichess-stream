from stockfish import Stockfish



stockfish = Stockfish(path="./stockfish_14.1_linux_x64")
print(stockfish.get_fen_position())
stockfish.set_fen_position("5r1k/5q1P/p7/2p1p3/1p1b2Q1/1Pp5/P3RP1R/6K1 w")

print(stockfish.get_board_visual())