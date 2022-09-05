import { React } from "react";
import "@elastic/react-search-ui-views/lib/styles/styles.css";
import 'mapbox-gl/dist/mapbox-gl.css';
import StoreLocator from "./components/StoreLocator";


export default function App() {
  return (
    <StoreLocator />
  );
}