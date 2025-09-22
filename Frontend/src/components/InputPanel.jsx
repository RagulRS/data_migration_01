// frontend/src/components/InputPanel.jsx
import React, { useState } from "react";
import {
  Paper,
  TextField,
  Typography,
  Button,
  Grid,
  Box,
  Divider,
} from "@mui/material";

const InputPanel = ({ setSnackbar }) => {
  const [form, setForm] = useState({
    studyId: "",
    siteId: "",
    siteCountry: "",
    subjects: "",
    targetSpec: null,
  });

  const [results, setResults] = useState(null);

  const handleChange = (e) => {
    setForm({ ...form, [e.target.name]: e.target.value });
  };

  const handleFile = (file) => {
    setForm({ ...form, targetSpec: file });
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    if (!form.targetSpec) {
      setSnackbar({
        open: true,
        message: "Upload target specification file",
        severity: "error",
      });
      return;
    }

    try {
      const fd = new FormData();
      fd.append("studyId", form.studyId);
      fd.append("siteId", form.siteId);
      fd.append("siteCountry", form.siteCountry);
      fd.append("subjects", form.subjects);
      fd.append("targetSpec", form.targetSpec);

      setSnackbar({ open: true, message: "Processing...", severity: "info" });

      const res = await fetch("http://127.0.0.1:5000/api/migrate", {
        method: "POST",
        body: fd,
      });

      if (!res.ok) {
        const txt = await res.text();
        throw new Error(txt || `HTTP ${res.status}`);
      }

      const data = await res.json();
      console.log("Response data:", data);
      setResults(data);
        
      setSnackbar({ open: true, message: "Completed", severity: "success" });
    } catch (err) {
      console.error(err);
      setSnackbar({
        open: true,
        message: "Error: " + err.message,
        severity: "error",
      });
    }
  };

console.log("Results:", results);
  return (
    <>
      <Paper sx={{ p: 4, borderRadius: 3, boxShadow: 3, mb: 3 }}>
        <Typography variant="h5" gutterBottom>
          Veeva Data Migration
        </Typography>
        <form onSubmit={handleSubmit}>
          <Grid container spacing={3}>
            <Grid item xs={12} sm={6}>
              <TextField
                label="Study ID"
                name="studyId"
                fullWidth
                value={form.studyId}
                onChange={handleChange}
                required
              />
            </Grid>
            <Grid item xs={12} sm={6}>
              <TextField
                label="Site ID"
                name="siteId"
                fullWidth
                value={form.siteId}
                onChange={handleChange}
                required
              />
            </Grid>
            <Grid item xs={12}>
              <TextField
                label="Site Country"
                name="siteCountry"
                fullWidth
                value={form.siteCountry}
                onChange={handleChange}
                required
              />
            </Grid>
            <Grid item xs={12}>
              <TextField
                label="Subjects mapping (old:new, comma separated)"
                name="subjects"
                fullWidth
                value={form.subjects}
                onChange={handleChange}
                placeholder="Example: SCR-0001:SCR-0053, SCR-0002:SCR-0054"
                required
              />
            </Grid>

            <Grid item xs={12}>
              <Typography>
                Upload Target Design Spec
              </Typography>
              <input
                type="file"
                accept=".csv, .xlsx, .xls"
                onChange={(e) => handleFile(e.target.files[0])}
              />
            </Grid>

            <Grid item xs={12}>
              <Box textAlign="center">
                <Button variant="contained" color="primary" type="submit">
                  Submit
                </Button>
              </Box>
            </Grid>
          </Grid>
        </form>
      </Paper>

      {results && (
        <Paper sx={{ p: 3, borderRadius: 2 }}>
          <Typography variant="h6">Comparison</Typography>
          <Divider sx={{ my: 1 }} />
          <Typography>
            <strong>Form mappings (target → source):</strong>
          </Typography>
          <pre style={{ whiteSpace: "pre-wrap" }}>
            {JSON.stringify(results.form_map, null, 2)}
          </pre>

          <Typography>
            <strong>Field mappings (per form):</strong>
          </Typography>
          <pre style={{ whiteSpace: "pre-wrap" }}>
            {JSON.stringify(results.field_map, null, 2)}
          </pre>

          <Typography sx={{ mt: 2 }}>
            <strong>Migration Log (first 50 rows):</strong>
          </Typography>
          <pre
            style={{ whiteSpace: "pre-wrap", maxHeight: 300, overflow: "auto" }}
          >
            {results?.migration_log
              ? JSON.stringify(results.migration_log.slice(0, 50), null, 2)
              : "No migration log available."}
          </pre>
        </Paper>
      )}
    </>
  );
};

export default InputPanel;
