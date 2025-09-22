// App.jsx
import React from "react";
import { ThemeProvider, createTheme } from "@mui/material/styles";
import { CssBaseline, Container, Box, Snackbar, Alert } from "@mui/material";
import useMediaQuery from "@mui/material/useMediaQuery";
import InputPanel from "./components/InputPanel";
import SidePanel from "./components/SidePanel";

// ✅ Error Boundary wrapper
class ErrorBoundary extends React.Component {
  constructor(props) {
    super(props);
    this.state = { hasError: false, error: null };
  }

  static getDerivedStateFromError(error) {
    return { hasError: true, error };
  }

  componentDidCatch(error, errorInfo) {
    console.error("Error Boundary caught an error:", error, errorInfo);
  }

  render() {
    if (this.state.hasError) {
      return (
        <Box sx={{ p: 4, textAlign: "center", color: "red" }}>
          <h2>Something went wrong.</h2>
          <p>{this.state.error?.message || "Unexpected error"}</p>
        </Box>
      );
    }
    return this.props.children;
  }
}

const App = () => {
  const prefersDarkMode = useMediaQuery("(prefers-color-scheme: dark)");

  const theme = React.useMemo(
    () =>
      createTheme({
        palette: {
          mode: prefersDarkMode ? "dark" : "light",
          primary: {
            main: "#1976d2",
          },
          background: {
            default: prefersDarkMode ? "#121212" : "#f5f5f5",
          },
        },
      }),
    [prefersDarkMode]
  );

  const [snackbar, setSnackbar] = React.useState({
    open: false,
    message: "",
    severity: "success",
  });

  return (
    <ThemeProvider theme={theme}>
      <CssBaseline />
      <Box sx={{ display: "flex", minHeight: "100vh" }}>
        <SidePanel />
        <Container sx={{ mt: 4, mb: 4, flexGrow: 1 }}>
          {/* ✅ Wrap InputPanel with ErrorBoundary */}
          <ErrorBoundary>
            <InputPanel setSnackbar={setSnackbar} />
          </ErrorBoundary>
        </Container>
      </Box>

      <Snackbar
        open={snackbar.open}
        autoHideDuration={4000}
        onClose={() => setSnackbar({ ...snackbar, open: false })}
        anchorOrigin={{ vertical: "bottom", horizontal: "center" }}
      >
        <Alert
          onClose={() => setSnackbar({ ...snackbar, open: false })}
          severity={snackbar.severity}
          sx={{ width: "100%" }}
        >
          {snackbar.message}
        </Alert>
      </Snackbar>
    </ThemeProvider>
  );
};

export default App;
