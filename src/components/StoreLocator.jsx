import {
    Grid
} from "@mui/material";
import { React, useState } from 'react'
import CitySearchBox from "./CitySearchBox";
import StoreResults from "./StoreResults";
import StoresMap from "./StoresMap";
import { SearchProvider } from '@elastic/react-search-ui'
import AppSearchAPIConnector from "@elastic/search-ui-app-search-connector";

const storesConnector = new AppSearchAPIConnector({
    searchKey: process.env.REACT_APP_APP_SEARCH_API_KEY,
    engineName: "stores",
    endpointBase: process.env.REACT_APP_APP_SEARCH_BASE_URL
});

const cityConnector = new AppSearchAPIConnector({
    searchKey: process.env.REACT_APP_APP_SEARCH_API_KEY,
    engineName: "cities",
    endpointBase: process.env.REACT_APP_APP_SEARCH_BASE_URL
});

const cityConfig = {
    alwaysSearchOnInitialLoad: false,
    apiConnector: cityConnector,
    searchQuery: {
        result_fields: {
            name: { raw: {} }
        },
        search_fields: {
            name: {}
        }
    },
    autocompleteQuery: {
        results: {
            resultsPerPage: 5,
            result_fields: {
                // specify the fields you want from the index to display the results
                name: { raw: {} },
                admin1_code: { raw: {} },
                coordinates: { raw: {} }
            },
            search_fields: {
                // specify the fields you want to search on
                name: {}
            }
        }
    },
};

const storesConfig = {
    apiConnector: storesConnector,
    trackUrlState: false,
    searchQuery: {
        search_fields: {
            location: {},
            name: {}
        },
        result_fields: {
            location: { raw: {} },
            name: { raw: {} },
            zip: { raw: {} },
            city: { raw: {} },
            state: { raw: {} },
            address: { raw: {} },
            hours: { raw: {} }
        }
    },
    alwaysSearchOnInitialLoad: false
};

const StoreLocator = () => {
    const [center, setCenter] = useState({
        lat: 39,
        lng: -98
    })

    const [searchTerm, setSearchTerm] = useState()
    const [zoom, setZoom] = useState(4)
    const [placeHover, setPlaceHover] = useState()

    const handleStoreSearch = (place) => {
        if (place.coordinates) {
            setSearchTerm(place.name.raw + ", " + place.admin1_code.raw)
            const lat = parseFloat(place.coordinates.raw.split(",")[0])
            const lon = parseFloat(place.coordinates.raw.split(",")[1])
            setCenter({ lat: lat, lng: lon })
            setZoom(10)
        }
    }

  

    return (
        <Grid container spacing={2} >
            <Grid item xs={3} style={{ maxHeight: "600px" }}>
                <SearchProvider config={cityConfig}>
                    <CitySearchBox query={searchTerm} handleStoreSearch={handleStoreSearch} />
                </SearchProvider>
                <SearchProvider config={storesConfig}>
                    <StoreResults center={center} setPlaceHover={setPlaceHover}/>
                </SearchProvider>
            </Grid>
            <Grid item xs={9}>
                <SearchProvider config={storesConfig}>
                    <StoresMap center={center} zoom={zoom} placeHover={placeHover} />
                </SearchProvider>
            </Grid>
        </Grid>
    )
}

export default StoreLocator;