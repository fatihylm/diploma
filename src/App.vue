<template>
  <div id="app">
    <DragDrop />
    <LogoVue />
    <AboutSection />
  </div>
</template>

<script>
import DragDrop from "@/components/DragDrop.vue";
import LogoVue from "./components/LogoImage.vue";
import AboutSection from "./components/AboutSection.vue";

export default {
  name: "App",
  components: {
    DragDrop,
    LogoVue,
    AboutSection,
  },
};
</script>
