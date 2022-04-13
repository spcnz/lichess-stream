const Timer = ({ seconds, style }) => {

    return (
        <div style={style}>{Math.floor(seconds / 60)}:{seconds % 60}</div>
    )
}

export default Timer;