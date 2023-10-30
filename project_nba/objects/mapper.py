import logging
from objects.player import Player

LOGGER = logging.getLogger(__name__)


def player_mapper(data: dict) -> Player:
    return Player(
        player=data.get("Player"),
        year=data.get("Year"),
        team=data.get("Tm"),
        pos=data.get("Pos"),
        age=data.get("Age"),
        gp=data.get("G"),
        gs=data.get("GS"),
        mp=data.get("MP"),
        fg=data.get("FG"),
        fga=data.get("FGA"),
        fgperc=data.get("FG%"),
        twop=data.get("2P"),
        twopa=data.get("2PA"),
        twopperc=data.get("2P%"),
        threep=data.get("3P"),
        threepa=data.get("3PA"),
        threepperc=data.get("3P%"),
        ft=data.get("FT"),
        fta=data.get("FTA"),
        ftperc=data.get("FT%"),
        orb=data.get("ORB"),
        drb=data.get("DRB"),
        trb=data.get("TRB"),
        ast=data.get("AST"),
        stl=data.get("STL"),
        blk=data.get("BLK"),
        tov=data.get("TOV"),
        pf=data.get("PF"),
        pts=data.get("PTS"),
        per=data.get("PER"),
        ts=data.get("TS%"),
        ows=data.get("OWS"),
        dws=data.get("DWS"),
        ws48=data.get("WS/48"),
        obpm=data.get("OBPM"),
        dbpm=data.get("DBPM"),
        bpm=data.get("BPM"),
        vorp=data.get("VORP"),
    )
