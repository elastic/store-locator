
import { Grid, List, ListItemAvatar, ListItem, Avatar, Card, CardContent, CardActions, Button, Typography } from "@mui/material";
import StoreIcon from '@mui/icons-material/Store';

import React, { useEffect } from "react";
import { withSearch } from "@elastic/react-search-ui";


const StoreResults = ({ center, results, setFilter, setSort, setPlaceHover }) => {

    useEffect(() => {
        setFilter("location", { center: center.lat + "," + center.lng, distance: 50, unit: "km" })
        setSort("location", { center: center.lat + "," + center.lng, order: "asc" })
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [center]);

    const handleOnResultHover = (event) => {
        setPlaceHover(event.target.id)
    }

    const handleOnResultHoverEnd = (event) => {
       setPlaceHover(null)
    }

    return (
        <Grid item style={{ maxHeight: "507px", overflow: 'auto' }}>
            <List>
                {results.map((r) => (
                    <ListItem key={r.id.raw}>
                        <ListItemAvatar>
                            <Avatar>
                                <StoreIcon fontSize="medium" />
                            </Avatar>
                        </ListItemAvatar>
                        <Card sx={{ width: 325 }}>
                            <CardContent>
                                <Typography sx={{ fontSize: 14 }} color="text.secondary" gutterBottom>
                                    {r.name.raw}
                                </Typography>
                                <Typography variant="body2">
                                    {r.address.raw}<br />
                                    {r.city.raw}, {r.state.raw}
                                </Typography>
                            </CardContent>
                            <CardActions id={r.id.raw} onMouseEnter={handleOnResultHover} onMouseLeave={handleOnResultHoverEnd}>
                                <Button size="small">Shop here</Button>
                            </CardActions>
                        </Card>
                    </ListItem>
                ))}
            </List>
        </Grid>
    );
};

export default withSearch(({ results, setFilter, setSort }) => ({
    results,
    setFilter,
    setSort
}))(StoreResults);

