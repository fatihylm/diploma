<template>
  <div id="app">
    <DragDrop />
    <LogoVue />
  </div>
</template>

<script>
import DragDrop from "@/components/DragDrop.vue";
import LogoVue from "./components/LogoImage.vue";

export default {
  name: "App",
  components: {
    DragDrop,
    LogoVue,
  },
};
</script>

<style>
#app {
  font-family: Avenir, Helvetica, Arial, sans-serif;
  text-align: center;
  color: #2c3e50;
  margin-top: 60px;
}
</style>
