from stockfish import Stockfish



stockfish = Stockfish(path="./stockfish_14.1_linux_x64")
print(stockfish.get_fen_position())
stockfish.set_position(["e2e4", "e7e6"])

print(stockfish.get_board_visual())