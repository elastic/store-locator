
import { React, useEffect, useRef } from 'react'
import Map, { Marker } from 'react-map-gl';
import { withSearch } from "@elastic/react-search-ui";

const StoresMap = ({ center, zoom, results, setFilter, placeHover }) => {

    const map = useRef(null);

    useEffect(() => {
        setFilter("location", { center: center.lat + "," + center.lng, distance: 50, unit: "km" })
        console.log(center)
        if (map.current) {
            map.current.jumpTo({center: [center.lng,  center.lat], zoom: 10});
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [center]);

   
    const getLat = (latLon) => {
        const latLonArr = latLon.split(",")
        const lat = parseFloat(latLonArr[0])
        return lat
    }

    const getLng = (latLon) => {
        const latLonArr = latLon.split(",")

        const lon = parseFloat(latLonArr[1])
        return lon
    }


    return (<Map
        ref={map}
        style={{ height: "800px" }}
        initialViewState={{
            longitude: center.lng,
            latitude: center.lat,
            zoom: zoom
        }}
        mapStyle="mapbox://styles/mapbox/streets-v11"
    >
        {results.map((r) => (
        placeHover === r.id.raw? 
        <Marker key={r.id.raw} longitude={getLng(r.location.raw)} latitude={getLat(r.location.raw)} anchor="bottom" >
            <img src="./mapbox-marker-icon-20px-blue.png" alt="" />
        </Marker>
        :
        <Marker key={r.id.raw} longitude={getLng(r.location.raw)} latitude={getLat(r.location.raw)} anchor="bottom" >
            <img src="./mapbox-marker-icon-20px-red.png" alt="" />
        </Marker>
        ))} 
    </Map>)

}

export default withSearch(({ results, setFilter }) => ({
    results,
    setFilter
}))(StoresMap);