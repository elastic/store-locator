

import React, { useEffect } from "react"; // styled components
import {  withSearch, SearchBox } from '@elastic/react-search-ui'
import {
    styled
} from "@mui/material";

const SearchBoxComponent = styled(SearchBox)(() => ({
    display: "block",
    padding: "16px"
}));

const SearchBoxInput = styled(`input`)(() => ({
    width: "100%",
    height: "3.4375rem",
    borderRadius: "0.375rem",
    border: "1px solid rgba(0,0,0,.5)",
    fontSize: ".875rem",
    fontWeight: "500",
    letterSpacing: ".2px",
    color: "#000",
    marginLeft: "16px",
    marginTop: "6px"
}));



const SearchBoxAutoComplete = styled('div')(() => ({
    top: "85%",
    marginLeft: "32px"

}));

const SearchBoxAutoCompleteItem = styled('li')(() => ({
    "&:hover": {
        cursor: "pointer",
    },
    padding: "8px",
    flex: "none",
    order: 4,
    alignSelf: "stretch",
    flexGrow: 0,
    margin: "0px 0px",
    display: "flex",
    flexDirection: "row",
    alignItems: "center",
    whiteSpace: "pre-wrap"
}));


const CitySearchBox = ({ query, setSearchTerm, handleStoreSearch }) => {

    useEffect(() => {
        if (query) setSearchTerm(query)
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [query]);

    const renderAutocompleteView = ({ autocompletedResults, className, getMenuProps, getItemProps }) => {
        return (
            <SearchBoxAutoComplete  {...getMenuProps({
                className: ["sui-search-box__autocomplete-container", className].join(
                    " "
                )
            })}>
                {
                    autocompletedResults.map((result, i) => {

                        return (

                            <SearchBoxAutoCompleteItem
                                {...getItemProps({
                                    key: 'query-' + result.id.raw,
                                    item: result
                                })}
                            >
                                {result.name.raw}, {result.admin1_code.raw}
                            </SearchBoxAutoCompleteItem>

                        )
                    })}
            </SearchBoxAutoComplete>
        )
    }

    return (

        <SearchBoxComponent
            inputView={({ getAutocomplete, getInputProps }) => (
                <div>
                    <SearchBoxInput
                        {...getInputProps({
                            placeholder: "Enter a city..."
                        })}
                    />
                    {getAutocomplete()}
                </div>
            )}
            autocompleteResults={{
                titleField: "name"
            }}
            onSelectAutocomplete={handleStoreSearch}
            autocompleteView={renderAutocompleteView}
        

        />


    )
}



export default withSearch(({ setSearchTerm }) => ({
    setSearchTerm
}))(CitySearchBox);


