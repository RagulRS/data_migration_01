import React from "react";
import { Drawer, List, ListItem, ListItemText, Toolbar, Typography } from "@mui/material";

const SidePanel = () => {
  return (
    <Drawer
      variant="permanent"
      sx={{
        width: 240,
        flexShrink: 0,
        [`& .MuiDrawer-paper`]: {
          width: 240,
          boxSizing: "border-box",
          backgroundColor: "#1e1e1e",
          color: "#fff"
        },
      }}
    >
      <Toolbar>
        <Typography variant="h6" noWrap>
          Data Tools
        </Typography>
      </Toolbar>
      <List>
        <ListItem button>
          <ListItemText primary="Uploader" />
        </ListItem>
        <ListItem button>
          <ListItemText primary="Specs" />
        </ListItem>
      </List>
    </Drawer>
  );
};

export default SidePanel;
