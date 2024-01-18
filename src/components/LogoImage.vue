<template>
    <div>
      <img :src="htldornbirn" alt="team" class="htl"/>
    </div>
  </template>
  
  <script>
  import htldornbirn from '/src/images/htldornbirn.png'

  export default {
    data() {
      return {
        htldornbirn:htldornbirn
      };
    }
  };
  </script>
  
  <style>
    @import '/src/css/LogoImageStyle.css';
 </style>
  